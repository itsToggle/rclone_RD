// Package realdebrid provides an interface to the real-debrid.com
// object storage system.
package realdebrid

/*
Run of rclone info
stringNeedsEscaping = []rune{
	0x00, 0x0A, 0x0D, 0x22, 0x2F, 0x5C, 0xBF, 0xFE
	0x00, 0x0A, 0x0D, '"',  '/',  '\\', 0xBF, 0xFE
}
maxFileLength = 255
canWriteUnnormalized = true
canReadUnnormalized   = true
canReadRenormalized   = false
canStream = true
*/

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync"
	"github.com/rclone/rclone/backend/realdebrid/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/oauthutil"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/oauth2"
)

const (
	rcloneClientID              = "X245A4XAIBGVM"
	rcloneEncryptedClientSecret = "B5YIvQoRIhcpAYs8HYeyjb9gK-ftmZEbqdh_gNfc4RgO9Q"
	minSleep                    = 10 * time.Millisecond
	maxSleep                    = 2 * time.Second
	decayConstant               = 2   // bigger for slower decay, exponential
	rootID                      = "/" // ID of root folder is always this
	rootURL                     = "https://api.real-debrid.com/rest/1.0"
)

// Globals
var (
	// Description of how to auth for this app
	oauthConfig = &oauth2.Config{
		Scopes: nil,
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://api.real-debrid.com/oauth/v2/auth",
			TokenURL: "https://api.real-debrid.com/oauth/v2/token",
		},
		ClientID:     rcloneClientID,
		ClientSecret: obscure.MustReveal(rcloneEncryptedClientSecret),
		RedirectURL:  oauthutil.RedirectURL,
	}
)

type RegexValuePair struct {
	Regex *regexp.Regexp
	Value string
}

//Global variables
var cached []api.Item
var torrents []api.Item
var broken_torrents []string
var lastcheck int64 = 0
var lastFileMod int64 = 0
var interval int64 = 15 * 60
var file_mutex = &sync.RWMutex{}
var move_mutex = &sync.RWMutex{}
var moving = false
var mapping = xsync.NewMap()
var sorting_file = xsync.NewMap()
var folders = xsync.NewMap()
var regex_defs = []RegexValuePair{}
var trash_indicator = ".trashed"
var move_chars = " -> "
var regx_chars = " == "
var default_sorting = `# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~ rclone_rd sorting file ~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# - write comment lines using "#"
#
# - write regex definitions using: "/foldername" + " == " + regex definition. You can edit the exising ones or create new ones.
#   Order matters for regex folders, first match will be final destination. Make sure there are no trailing space characters.
#   torrents that dont match any regex definition end up in a folder named "default".
#   Example: /movies == (?i)(19|20)([0-9]{2} ?\.?)
#
# - create new directories using "/foldername"
#   Example: /shit
#
# - write move/renaming changes using: "/" + "actual torrent title" + "/" + "file ID" + " -> " + "destination"
#   You do not need to create the directories you are moving stuff to, this will be done automatically.
#   Example: /some.show.S01/ -> /shows/some.show/season 1/
#   Example: /some.show.S01/ABCDEFGHIJKL -> /shows/some.show/season 1/episode 1.mkv

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~ top level and regex folders: ~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

/shows == (?i)(S[0-9]{2}|SEASONS?.[0-9]|COMPLETE|[^457a-z\W\s]-[0-9]+)
/movies == (?i)(19|20)([0-9]{2} ?\.?)
/default

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~ recorded/manual changes to the structure: ~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


`

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "realdebrid",
		Description: "real-debrid.com",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:    "api_key",
			Help:    `please provide your RealDebrid API key.`,
			Default: "",
		}, {
			Name:     "sort_file",
			Help:     `please provide the full path to a file (file does not need to exist) that should be used for sorting`,
			Advanced: true,
			Default:  path.Join(path.Dir(config.GetConfigPath()), "sorting.txt"),
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// Encode invalid UTF-8 bytes as json doesn't handle them properly.
			Default: (encoder.Display |
				encoder.EncodeBackSlash |
				encoder.EncodeDoubleQuote |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	SortFile string               `config:"sort_file"`
	APIKey   string               `config:"api_key"`
	Enc      encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote cloud storage system
type Fs struct {
	name         string             // name of this remote
	root         string             // the path we are working on
	opt          Options            // parsed options
	features     *fs.Features       // optional features
	srv          *rest.Client       // the connection to the server
	dirCache     *dircache.DirCache // Map of directory path to directory id
	pacer        *fs.Pacer          // pacer for API calls
	tokenRenewer *oauthutil.Renew   // renew the token on expiry
}

// Object describes a file
type Object struct {
	fs           *Fs       // what this object is part of
	remote       string    // The remote path
	hasMetaData  bool      // metadata is present and correct
	size         int64     // size of the object
	modTime      time.Time // modification time of the object
	id           string    // ID of the object
	ParentID     string    // ID of parent directory
	mimeType     string    // Mime type of object
	url          string    // URL to download file
	TorrentHash  string    // Torrent Hash
	MappingID    string    // Internal Mapping ID used for sorting
	OriginalLink string    //Internal original Link
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("realdebrid root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// parsePath parses a realdebrid 'url'
func parsePath(path string) (root string) {
	root = strings.Trim(path, "/")
	return
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// readMetaDataForPath reads the metadata from the path
func (f *Fs) readMetaDataForPath(ctx context.Context, path string, directoriesOnly bool, filesOnly bool) (info *api.Item, err error) {
	// defer fs.Trace(f, "path=%q", path)("info=%+v, err=%v", &info, &err)
	leaf, directoryID, err := f.dirCache.FindPath(ctx, path, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	if len(directoryID) > 0 && directoryID != "0" {
		if last := directoryID[len(directoryID)-1]; last != '/' {
			directoryID = directoryID + "/"
		}
	}
	lcLeaf := strings.ToLower(leaf)
	_, found, err := f.listAll(ctx, directoryID, directoriesOnly, filesOnly, func(item *api.Item) bool {
		if strings.ToLower(item.Name) == lcLeaf {
			info = item
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return info, nil
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		body = nil
	}
	var e = api.Response{
		Message: string(body),
		Status:  fmt.Sprintf("%s (%d)", resp.Status, resp.StatusCode),
	}
	if body != nil {
		_ = json.Unmarshal(body, &e)
	}
	return &e
}

// Return a url.Values with the api key in
func (f *Fs) baseParams() url.Values {
	params := url.Values{}
	if f.opt.APIKey != "" {
		params.Add("auth_token", f.opt.APIKey)
	}
	return params
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	root = parsePath(root)

	var client *http.Client
	var ts *oauthutil.TokenSource
	if opt.APIKey == "" {
		client, ts, err = oauthutil.NewClient(ctx, name, m, oauthConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to configure realdebrid: %w", err)
		}
	} else {
		client = fshttp.NewClient(ctx)
	}

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		srv:   rest.NewClient(client).SetRoot(rootURL),
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
	}).Fill(ctx, f)
	f.srv.SetErrorHandler(errorHandler)

	// Renew the token in the background
	if ts != nil {
		f.tokenRenewer = oauthutil.NewRenew(f.String(), ts, func() error {
			_, err := f.About(ctx)
			return err
		})
	}

	// Get rootID
	f.dirCache = dircache.New(root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.newObjectWithInfo(ctx, remote, nil)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.Item) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	var err error
	if info != nil {
		// Set info
		err = o.setMetaData(info)
	} else {
		err = o.readMetaData(ctx) // reads info and meta, returning an error
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// Find the leaf in pathID
	var newDirID string
	newDirID, found, err = f.listAll(ctx, pathID, true, false, func(item *api.Item) bool {
		if strings.EqualFold(item.Name, leaf) {
			pathIDOut = item.ID
			return true
		}
		return false
	})
	// Update the Root directory ID to its actual value
	if pathID == rootID {
		f.dirCache.SetRootIDAlias(newDirID)
	}
	return pathIDOut, found, err
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, dirID, leaf string) (newID string, err error) {
	// fs.LogPrint(fs.LogLevelDebug, "CreateDir locking file_mutex")
	file_mutex.Lock()
	defer file_mutex.Unlock()
	// defer fs.LogPrint(fs.LogLevelDebug, "CreateDir unlocking file_mutex")
	if len(dirID) > 0 && dirID != "/" {
		if first := dirID[0]; first != '/' {
			dirID = "/" + dirID
		}
		if last := dirID[len(dirID)-1]; last != '/' {
			dirID = dirID + "/"
		}
	} else {
		err := fmt.Errorf("cant create directories in root directory. this is reserved for regex folders")
		return "", err
	}
	if last := leaf[len(leaf)-1]; last != '/' {
		leaf = leaf + "/"
	}
	file, err := os.OpenFile(f.opt.SortFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	defer file.Close()
	_, err = file.WriteString(dirID + leaf + "\n")
	if err != nil {
		fmt.Println(err)
		return dirID + leaf, err
	}
	return dirID + leaf, nil
}

// Redownload a dead torrent
func (f *Fs) redownloadTorrent(ctx context.Context, torrent api.Item) (redownloaded_torrent api.Item) {
	fs.LogPrint(fs.LogLevelNotice, "Redownloading dead torrent: "+torrent.Name)
	//Get dead torrent file and hash info
	var method = "GET"
	var path = "/torrents/info/" + torrent.ID
	var opts = rest.Opts{
		Method:     method,
		Path:       path,
		Parameters: f.baseParams(),
	}
	_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
	var selected_files []int64
	var dead_torrent_id = torrent.ID
	for _, file := range torrent.Files {
		if file.Selected == 1 {
			selected_files = append(selected_files, file.ID)
		}
	}
	var selected_files_str = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(selected_files)), ","), "[]")
	//Delete old download links
	for _, link := range torrent.Links {
		for i, cachedfile := range cached {
			if cachedfile.OriginalLink == link {
				path = "/downloads/delete/" + cachedfile.ID
				opts = rest.Opts{
					Method:     "DELETE",
					Path:       path,
					Parameters: f.baseParams(),
				}
				var resp *http.Response
				var result api.Response
				var retries = 0
				var err_code = 0
				resp, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
				if resp != nil {
					err_code = resp.StatusCode
				}
				for err_code == 429 && retries <= 5 {
					time.Sleep(time.Duration(2) * time.Second)
					resp, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
					if resp != nil {
						err_code = resp.StatusCode
					}
					retries += 1
				}
				cached[i].OriginalLink = "this-is-not-a-link"
			}
		}
	}
	//Add torrent again
	path = "/torrents/addMagnet"
	method = "POST"
	opts = rest.Opts{
		Method: method,
		Path:   path,
		MultipartParams: url.Values{
			"magnet": {"magnet:?xt=urn:btih:" + torrent.TorrentHash},
		},
		Parameters: f.baseParams(),
	}
	_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
	method = "GET"
	path = "/torrents/info/" + torrent.ID
	opts = rest.Opts{
		Method:     method,
		Path:       path,
		Parameters: f.baseParams(),
	}
	_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
	var tries = 0
	for torrent.Status != "waiting_files_selection" && tries < 5 {
		time.Sleep(time.Duration(1) * time.Second)
		_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
		tries += 1
	}
	//Select the same files again
	path = "/torrents/selectFiles/" + torrent.ID
	method = "POST"
	opts = rest.Opts{
		Method: method,
		Path:   path,
		MultipartParams: url.Values{
			"files": {selected_files_str},
		},
		Parameters: f.baseParams(),
	}
	_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
	//Delete the old torrent
	path = "/torrents/delete/" + dead_torrent_id
	method = "DELETE"
	opts = rest.Opts{
		Method:     method,
		Path:       path,
		Parameters: f.baseParams(),
	}
	_, _ = f.srv.CallJSON(ctx, &opts, nil, &torrent)
	torrent.Status = "downloaded"
	lastcheck = time.Now().Unix() - interval
	for i, TorrentID := range broken_torrents {
		if dead_torrent_id == TorrentID {
			broken_torrents[i] = broken_torrents[len(broken_torrents)-1]
			broken_torrents = broken_torrents[:len(broken_torrents)-1]
		}
	}
	return torrent
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) folder_exists(dirID string) bool {
	if dirID == "/" {
		return false
	}
	if _, ok := folders.Load(dirID); ok {
		return true
	}
	return false
}

// Clean sync.map without creating race conditions
func eraseSyncMap(m *xsync.Map) {
	m.Range(func(key string, value interface{}) bool {
		m.Delete(key)
		return true
	})
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
// User function to process a File item from listAll
//
// Should return true to finish processing
type listAllFn func(*api.Item) bool

// Lists the directory required calling the user function on each item found
//
// If the user fn ever returns true then it early exits with found = true
//
// It returns a newDirID which is what the system returned as the directory ID
func (f *Fs) listAll(ctx context.Context, dirID string, directoriesOnly bool, filesOnly bool, fn listAllFn) (newDirID string, found bool, err error) {

	path := "/downloads"
	method := "GET"
	var partialresult []api.Item
	var result []api.Item
	var resp *http.Response

	if _, ok := folders.Load(dirID); !ok {
		if _, ok := folders.Load(dirID + "/"); ok {
			dirID = dirID + "/"
		} else if _, ok := folders.Load("/" + dirID + "/"); ok {
			dirID = "/" + dirID + "/"
		} else if len(dirID) > 1 {
			if last := dirID[len(dirID)-1]; last == '/' {
				if _, ok := folders.Load(dirID[:len(dirID)-1]); ok {
					dirID = dirID[:len(dirID)-1]
				}
			}
		}
	}
	// Check the sorting file for updates
	fileModTime := time.Now().Unix()
	if math.Abs(float64(fileModTime-lastFileMod)) >= 5 {
		fileInfo, err := os.Stat(f.opt.SortFile)
		if os.IsNotExist(err) {
			// file does not exist
		} else if err != nil {
			// error occurred while checking file info
		} else {
			// file exists, get modification time
			fileModTime = fileInfo.ModTime().Unix()
		}
	}
	var updated = false
	if fileModTime > lastFileMod {
		updated = true
	}

	if ((dirID == rootID) || !(f.folder_exists(dirID)) || updated) && !moving {
		// Create folder structure
		if updated {
			// fs.LogPrint(fs.LogLevelDebug, "ListAll Rlocking file_mutex")
			file_mutex.RLock()
			// defer fs.LogPrint(fs.LogLevelDebug, "ListAll Runlocking file_mutex")
			// read the file, create if missing
			file, err := os.Open(f.opt.SortFile)
			if os.IsNotExist(err) {
				fs.LogPrint(fs.LogLevelWarning, "no sorting file found - creating new empty sorting file")
				file, err = os.OpenFile(f.opt.SortFile, os.O_CREATE|os.O_RDWR, 0644)
				if err != nil {
					fmt.Println(err)
				}
				defer file.Close()

				if _, err := file.WriteString(default_sorting); err != nil {
					fmt.Println(err)
				}
				file.Sync() // flush file contents to disk
			} else if err != nil {
				fmt.Println(err)
			} else {
				defer file.Close()
			}

			// Reset saved folder structure
			fs.LogPrint(fs.LogLevelDebug, "reading updated sorting file")
			regex_defs = []RegexValuePair{}
			eraseSyncMap(folders)
			eraseSyncMap(mapping)
			eraseSyncMap(sorting_file)

			// Read the file line by line
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				fmt.Println(err)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "#") {
					continue
				} else if len(scanner.Text()) == 0 || scanner.Text() == "\n" || scanner.Text() == "\n\r" {
					continue
				} else if strings.Contains(scanner.Text(), move_chars) {
					// Split the line by " -> "
					parts := strings.Split(scanner.Text(), move_chars)
					// Add the key-value pair to the map if not present or changed
					oldValue, ok := mapping.Load(parts[0])
					if !ok || oldValue.(string) != parts[1] {
						mapping.Store(parts[0], parts[1])
					}
				} else if strings.Contains(scanner.Text(), regx_chars) {
					// Split the line by " == "
					parts := strings.Split(scanner.Text(), regx_chars)
					// Add the key-value pair to the map
					r, _ := regexp.Compile(parts[1])
					regex_defs = append(regex_defs, RegexValuePair{r, parts[0]})

				} else {
					oldValue, ok := mapping.Load(scanner.Text())
					if !ok || oldValue.(string) != scanner.Text() {
						mapping.Store(scanner.Text(), scanner.Text())
					}
				}
			}

			file_mutex.RUnlock()

			mapping.Range(func(key string, value interface{}) bool {
				oldValue, ok := sorting_file.Load(key)
				if !ok || oldValue != value {
					sorting_file.Store(key, value)
				}
				return true
			})

		}
		//update global cached list
		opts := rest.Opts{
			Method:     method,
			Path:       path,
			Parameters: f.baseParams(),
		}
		opts.Parameters.Set("includebreadcrumbs", "false")
		opts.Parameters.Set("limit", "1")
		var newcached []api.Item
		var totalcount int
		var printed = false
		var err_code = 429
		totalcount = 2
		for len(newcached) < totalcount {
			partialresult = nil
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
			var retries = 0
			if resp != nil {
				err_code = resp.StatusCode
			}
			for err_code == 429 && retries <= 5 {
				partialresult = nil
				time.Sleep(time.Duration(2) * time.Second)
				resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
				if resp != nil {
					err_code = resp.StatusCode
				}
				retries += 1
			}
			if err == nil {
				totalcount, err = strconv.Atoi(resp.Header["X-Total-Count"][0])
				if err == nil {
					if totalcount != len(cached) || time.Now().Unix()-lastcheck > interval {
						if time.Now().Unix()-lastcheck > interval && !printed {
							fs.LogPrint(fs.LogLevelDebug, "updating all links and torrents")
							printed = true
						}
						newcached = append(newcached, partialresult...)
						opts.Parameters.Set("offset", strconv.Itoa(len(newcached)))
						opts.Parameters.Set("limit", "2500")
						// fs.LogPrint(fs.LogLevelDebug, "Setting updated to true")
						updated = true
					} else {
						newcached = cached
					}
				} else {
					break
				}
			} else {
				break
			}
		}
		//fmt.Printf("Done.\n")
		//fmt.Printf("Updating RealDebrid Torrents ... ")
		cached = newcached
		//get torrents
		path = "/torrents"
		opts = rest.Opts{
			Method:     method,
			Path:       path,
			Parameters: f.baseParams(),
		}
		opts.Parameters.Set("limit", "1")
		var newtorrents []api.Item
		totalcount = 2
		err_code = 429
		for len(newtorrents) < totalcount {
			partialresult = nil
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
			if resp != nil {
				err_code = resp.StatusCode
			}
			var retries = 0
			for err_code == 429 && retries <= 5 {
				partialresult = nil
				time.Sleep(time.Duration(2) * time.Second)
				resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
				if resp != nil {
					err_code = resp.StatusCode
				}
				retries += 1
			}
			if err == nil {
				totalcount, err = strconv.Atoi(resp.Header["X-Total-Count"][0])
				if err == nil {
					if totalcount != len(torrents) || time.Now().Unix()-lastcheck > interval {
						newtorrents = append(newtorrents, partialresult...)
						opts.Parameters.Set("offset", strconv.Itoa(len(newtorrents)))
						opts.Parameters.Set("limit", "2500")
						// fs.LogPrint(fs.LogLevelDebug, "Setting updated to true")
						updated = true
					} else {
						newtorrents = torrents
					}
				} else {
					break
				}
			} else {
				break
			}
		}

		// Set everything as being up to date
		lastcheck = time.Now().Unix()
		lastFileMod = fileModTime
		//fmt.Printf("Done.\n")
		torrents = newtorrents

		// Iterate through built file and torrent list:
		var broken = false
		err_code = 0
		if updated {
			for i := range torrents {
				//handle dead torrents
				broken = false
				for _, TorrentID := range broken_torrents {
					if torrents[i].ID == TorrentID {
						broken = true
					}
				}
				if torrents[i].Status == "dead" || torrents[i].Status == "error" || broken {
					torrents[i] = f.redownloadTorrent(ctx, torrents[i])
				}
				//set default torrents[i] location
				torrents[i].DefaultLocation = "/default/"
				for _, pair := range regex_defs {
					match := pair.Regex.MatchString(torrents[i].Name)
					if match {
						torrents[i].DefaultLocation = pair.Value + "/"
						break
					}
				}
				//iterate through files
				for _, link := range torrents[i].Links {
					err_code = 0
					if link == "" {
						continue
					}
					var ItemFile api.Item
					ItemFile.Name = strings.Split(link, "/")[len(strings.Split(link, "/"))-1]
					ItemFile.ID = ItemFile.Name
					mapping_id := "/" + torrents[i].Name + "/" + ItemFile.ID
					ItemFile.MappingID = mapping_id
					ItemFile.DefaultLocation = torrents[i].DefaultLocation + torrents[i].Name + "/"
					ItemFile.ParentID = torrents[i].ID
					ItemFile.TorrentHash = torrents[i].TorrentHash
					ItemFile.Generated = torrents[i].Generated
					ItemFile.Ended = torrents[i].Ended
					ItemFile.Type = "file"
					ItemFile.Link = link
					ItemFile.OriginalLink = link
					if _, ok := mapping.Load(mapping_id); !ok {
						if _, ok := mapping.Load("/" + torrents[i].Name + "/"); ok {
							value, _ := mapping.Load("/" + torrents[i].Name + "/")
							mapping.Store(mapping_id, value)
						} else {
							mapping.Store(mapping_id, ItemFile.DefaultLocation)
						}
					} else {
						if value, _ := mapping.Load(mapping_id); len(value.(string)) > 0 {
							value, _ := mapping.Load(mapping_id)
							split := strings.Split(value.(string), "/")
							if len(split) > 0 {
								ItemFile.Name = split[len(strings.Split(value.(string), "/"))-1]
							}
						}
						if ItemFile.Name == "" || strings.HasSuffix(ItemFile.Name, trash_indicator) {
							continue
						}
						value, _ := mapping.Load(mapping_id)
						mapping.Store(mapping_id, strings.Join(strings.Split(value.(string), "/")[:len(strings.Split(value.(string), "/"))-1], "/")+"/")
					}
					value, _ := mapping.Load(mapping_id)
					if value != nil {
						list, ok := folders.Load(value.(string))
						if !ok {
							list = []api.Item{}
						}
						skip := false
						for _, existing_item := range list.([]api.Item) {
							if existing_item.Name == ItemFile.Name {
								skip = true
								break
							}
						}
						if skip {
							continue
						}
						folders.Store(value.(string), append(list.([]api.Item), ItemFile))
					}
				}
			}

			// Iterate through the map
			mapping.Range(func(key string, newLocation interface{}) bool {

				if strings.HasSuffix(newLocation.(string), trash_indicator) {
					return true
				}

				// Split the new location by "/"
				locationParts := strings.Split(newLocation.(string), "/")

				// Create a new variable to store the full path
				var location string

				// Iterate through each level of the new location
				for _, locationPart := range locationParts[:len(locationParts)-1] {
					// Append the full path to the corresponding level
					var skip bool
					skip = false
					list, ok := folders.Load(location)
					if !ok {
						list = []api.Item{}
					}
					for _, val := range list.([]api.Item) {
						if val.Name == locationPart {
							skip = true
							break
						}
					}
					if len(location) > 0 {
						if last := location[len(location)-1]; last != '/' {
							skip = true
						}
					} else {
						skip = true
					}
					if skip {
						location = location + locationPart + "/"
						continue
					}

					// Create the missing folders
					var ItemFolder api.Item
					ItemFolder.Name = locationPart
					ItemFolder.ID = location + locationPart
					ItemFolder.Type = "folder"
					if locationPart != "" {
						folders.Store(location, append(list.([]api.Item), ItemFolder))
					}
					// Append the current location part to the full path
					location = location + locationPart + "/"
				}
				return true
			})

		}

	}

	value, _ := folders.Load(dirID)
	if value == nil {
		value = []api.Item{}
	}

	result = append(result, value.([]api.Item)...)

	if err != nil {
		return newDirID, found, fmt.Errorf("couldn't list files: %w", err)
	}
	loc, _ := time.LoadLocation("Europe/Paris")
	for i := range result {
		// Turn temporary restricted items into unretricted items
		if result[i].Type == api.ItemTypeFile {
			expired := true
			for _, cachedfile := range cached {
				if cachedfile.OriginalLink == result[i].OriginalLink {
					result[i].Name = cachedfile.Name
					result[i].Link = cachedfile.Link
					result[i].Size = cachedfile.Size
					expired = false
					break
				}
			}
			if expired {
				fs.LogPrint(fs.LogLevelDebug, fmt.Sprintf("creating new link for file %s from torrent hash %s", result[i].Name, result[i].TorrentHash))
				var ItemFile api.Item
				broken := false
				path = "/unrestrict/link"
				method = "POST"
				opts := rest.Opts{
					Method: method,
					Path:   path,
					MultipartParams: url.Values{
						"link": {result[i].OriginalLink},
					},
					Parameters: f.baseParams(),
				}
				err_code := 0
				resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
				if resp != nil {
					err_code = resp.StatusCode
				}
				if err_code == 503 || err_code == 404 {
					broken = true
				}
				if !broken {
					var retries = 0
					for err_code == 429 && retries <= 5 {
						time.Sleep(time.Duration(2) * time.Second)
						resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
						if resp != nil {
							err_code = resp.StatusCode
						}
						retries += 1
					}
					if ItemFile.Link != "" && ItemFile.Name != "" {
						result[i].Name = ItemFile.Name
						result[i].Link = ItemFile.Link
						result[i].Size = ItemFile.Size
					}
				} else {
					for k, torrent := range torrents {
						if torrent.ID == result[i].ParentID {
							torrents[k] = f.redownloadTorrent(ctx, torrents[k])
							break
						}
					}
				}

			}
			if _, ok := sorting_file.Load(result[i].MappingID); ok {
				if value, _ := sorting_file.Load(result[i].MappingID); len(value.(string)) > 0 {
					value, _ := sorting_file.Load(result[i].MappingID)
					split := strings.Split(value.(string), "/")
					if len(split) > 0 {
						result[i].Name = split[len(strings.Split(value.(string), "/"))-1]
					}
				}
			}
		}
		item := &result[i]
		layout := "2006-01-02T15:04:05.000Z"
		if item.Generated != "" {
			t, _ := time.ParseInLocation(layout, item.Ended, loc)
			item.CreatedAt = t.Unix()
		} else if item.Ended != "" {
			t, _ := time.ParseInLocation(layout, item.Ended, loc)
			item.CreatedAt = t.Unix()
		}
		if item.Type == api.ItemTypeFolder {
			if filesOnly {
				continue
			}
		} else if item.Type == api.ItemTypeFile {
			if directoriesOnly {
				continue
			}
		} else {
			fs.Debugf(f, "Ignoring %q - unknown type %q", item.Name, item.Type)
			continue
		}
		item.Name = f.opt.Enc.ToStandardName(item.Name)
		if fn(item) {
			found = true
			break
		}
	}
	return
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	//fmt.Println("Listing Items ... ")
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	_, _, err = f.listAll(ctx, directoryID, false, false, func(info *api.Item) bool {
		remote := path.Join(dir, info.Name)
		if info.Type == api.ItemTypeFolder {
			// cache the directory ID for later lookups
			f.dirCache.Put(remote, info.ID)
			d := fs.NewDir(remote, time.Unix(info.CreatedAt, 0)).SetID(info.ID)
			entries = append(entries, d)
		} else if info.Type == api.ItemTypeFile {
			o, err := f.newObjectWithInfo(ctx, remote, info)
			if err != nil {
				iErr = err
				return true
			}
			entries = append(entries, o)
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if iErr != nil {
		return nil, iErr
	}
	//fmt.Println("Done Listing Items.")
	return entries, nil
}

// Creates from the parameters passed in a half finished Object which
// must have setMetaData called on it
//
// Returns the object, leaf, directoryID and error
//
// Used to create new objects
func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (o *Object, leaf string, directoryID string, err error) {
	// Create the directory for the object if it doesn't exist
	leaf, directoryID, err = f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return
	}
	// Temporary Object under construction
	o = &Object{
		fs:     f,
		remote: remote,
	}
	return o, leaf, directoryID, nil
}

// Put the object
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existingObj, err := f.newObjectWithInfo(ctx, src.Remote(), nil)
	switch err {
	case nil:
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		// Not found so create it
		return f.PutUnchecked(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// PutUnchecked the object into the container
//
// This will produce an error if the object already exists
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	o, _, _, err := f.createObject(ctx, remote, modTime, size)
	if err != nil {
		return nil, err
	}
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	if err != nil {
		return err
	}
	return nil
}

// purgeCheck removes the root directory, if check is set then it
// refuses to do so if it has anything in
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	dc := f.dirCache
	rootID, err := dc.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	// if rootID is a torrent ID
	if len(rootID) == 13 && rootID == strings.ToUpper(rootID) {
		fs.LogPrint(fs.LogLevelDebug, "removing realdebrid torrent id: "+rootID)
		path := "/torrents/delete/DISABLED" + rootID
		opts := rest.Opts{
			Method:     "DELETE",
			Path:       path,
			Parameters: f.baseParams(),
		}
		var resp *http.Response
		var result api.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &result)
			return shouldRetry(ctx, resp, err)
		})
		lastcheck = time.Now().Unix() - interval
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	//fmt.Printf("Rmdir: '%s'\n", dir)
	return f.purgeCheck(ctx, dir, true)
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Purge deletes all the files in the directory
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	//fmt.Printf("Purge: '%s'\n", dir)
	return f.purgeCheck(ctx, dir, false)
}

// move a file or folder
//
func (f *Fs) move(ctx context.Context, isFile bool, id, oldLeaf, newLeaf, oldDirectoryID, newDirectoryID string) (err error) {

	// Handle IDs
	oldDirectoryID_o := oldDirectoryID
	for _, torrent := range torrents {
		if torrent.ID == oldDirectoryID {
			oldDirectoryID = "/" + torrent.Name + "/"
			break
		} else if !isFile && torrent.Name == oldLeaf {
			oldDirectoryID = "/"
			break
		}
	}
	// // Handle Files
	if isFile && len(id) == 13 {
		oldLeaf = id
	}

	// There was a change, so update the file requiring a lock
	// fs.LogPrint(fs.LogLevelDebug, "move locking file_mutex")
	file_mutex.Lock()
	defer file_mutex.Unlock()
	// defer fs.LogPrint(fs.LogLevelDebug, "move unlocking file_mutex")
	// Open the file
	file, err := os.OpenFile(f.opt.SortFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	if last := newDirectoryID[len(newDirectoryID)-1]; last != '/' {
		newDirectoryID = newDirectoryID + "/"
	}
	if first := newDirectoryID[0]; first != '/' {
		newDirectoryID = "/" + newDirectoryID
	}
	if last := oldDirectoryID[len(oldDirectoryID)-1]; last != '/' {
		oldDirectoryID = oldDirectoryID + "/"
	}
	if first := oldDirectoryID[0]; first != '/' {
		oldDirectoryID = "/" + oldDirectoryID
	}
	if last := oldDirectoryID_o[len(oldDirectoryID_o)-1]; last != '/' {
		oldDirectoryID_o = oldDirectoryID_o + "/"
	}
	if first := oldDirectoryID_o[0]; first != '/' {
		oldDirectoryID_o = "/" + oldDirectoryID_o
	}
	if !isFile {
		if last := newLeaf[len(newLeaf)-1]; last != '/' {
			newLeaf = newLeaf + "/"
		}
		if last := oldLeaf[len(oldLeaf)-1]; last != '/' {
			oldLeaf = oldLeaf + "/"
		}
	}

	// fs.LogPrint(fs.LogLevelDebug, fmt.Sprintf("moving oldLeaf %s from oldDirectoryID %s to newLeaf %s newDirectoryID %s", oldLeaf, oldDirectoryID, newLeaf, newDirectoryID))

	// Get all files that should be moved.
	var affected_items []string
	sorting_file.Range(func(key string, value interface{}) bool {
		if strings.Contains(value.(string), oldDirectoryID+oldLeaf) {
			affected_items = append(affected_items, key)
		}
		return true
	})
	if len(affected_items) == 0 || !isFile {
		affected_items = append(affected_items, oldDirectoryID+oldLeaf)
	}
	scanner := bufio.NewScanner(file)
	var lines []string
	var new_lines = make(map[string]string)
	var replaced = false
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return err
	}
	file.Truncate(0)
	file.Seek(0, 0)
	// move all affected items
	for _, affected_item := range affected_items {
		for _, line := range lines {
			if strings.Contains(line, affected_item+move_chars) && !isFile {
				new_dst := strings.Replace(strings.Split(line, move_chars)[len(strings.Split(line, move_chars))-1], oldDirectoryID_o+oldLeaf, newDirectoryID+newLeaf, -1)
				new_lines[line] = affected_item + move_chars + new_dst
				replaced = true
			} else if strings.Contains(line, affected_item+move_chars) && isFile {
				new_lines[line] = affected_item + move_chars + newDirectoryID + newLeaf
				replaced = true
			} else if line == affected_item {
				new_lines[line] = newDirectoryID + newLeaf
				replaced = true
			}
		}
		if !replaced {
			var line = affected_item + move_chars + newDirectoryID + newLeaf
			lines = append(lines, line)
		}
	}
	for _, line := range lines {
		write := line
		if _, ok := new_lines[line]; ok {
			write = new_lines[line]
		}
		_, err := file.WriteString(write + "\n")
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {

	move_mutex.Lock()
	defer move_mutex.Unlock()
	moving = true
	defer func() { moving = false }()

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	// Create temporary object
	_, leaf, directoryID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	// Do the move
	err = f.move(ctx, true, srcObj.id, path.Base(srcObj.remote), leaf, srcObj.ParentID, directoryID)
	if err != nil {
		return nil, err
	}

	// Remove the old item from the folder structure
	oldDir, _ := mapping.LoadAndDelete(srcObj.MappingID)
	var newItems []api.Item
	items, _ := folders.Load(oldDir.(string))
	for i := range items.([]api.Item) {
		if items.([]api.Item)[i].ID != srcObj.id {
			newItems = append(newItems, items.([]api.Item)[i])
		}
	}
	folders.Store(oldDir.(string), newItems)

	// Add the new item to the folder structure
	if last := directoryID[len(directoryID)-1]; last != '/' {
		directoryID = directoryID + "/"
	}
	if first := directoryID[0]; first != '/' {
		directoryID = "/" + directoryID
	}
	mapping.Store(srcObj.MappingID, directoryID)
	newItem := api.Item{}
	newItem.Name = strings.Split(remote, "/")[len(strings.Split(remote, "/"))-1]
	newItem.Size = srcObj.size
	newItem.ID = srcObj.id
	newItem.MimeType = srcObj.mimeType
	newItem.Link = srcObj.url
	newItem.OriginalLink = srcObj.OriginalLink
	newItem.ParentID = srcObj.ParentID
	newItem.TorrentHash = srcObj.TorrentHash
	newItem.MappingID = srcObj.MappingID
	newItem.Type = "file"
	newItem.Generated = srcObj.modTime.Format("2006-01-02T15:04:05.000Z")
	items, _ = folders.Load(directoryID)
	if items == nil {
		items = []api.Item{}
	}
	items = items.([]api.Item)
	items = append(items.([]api.Item), newItem)
	folders.Store(directoryID, items)

	// moving = false
	// err = dstObj.readMetaData(ctx)

	if err != nil && !strings.HasSuffix(remote, trash_indicator) {
		return nil, err
	}
	return srcObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	move_mutex.Lock()
	defer move_mutex.Unlock()

	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	srcID, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	// Do the move
	err = f.move(ctx, false, srcID, srcLeaf, dstLeaf, srcDirectoryID, dstDirectoryID)
	if err != nil {
		return err
	}

	// List the files to again to make them visible in the new location instantly
	if last := dstDirectoryID[len(dstDirectoryID)-1]; last != '/' {
		dstDirectoryID = dstDirectoryID + "/"
	}
	if first := dstDirectoryID[0]; first != '/' {
		dstDirectoryID = "/" + dstDirectoryID
	}

	f.List(ctx, dstDirectoryID+dstLeaf)

	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

// PublicLink adds a "readable by anyone with link" permission on the given file or folder.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	_, err := f.dirCache.FindDir(ctx, remote, false)
	if err == nil {
		return "", fs.ErrorCantShareDirectories
	}
	o, err := f.NewObject(ctx, remote)
	if err != nil {
		return "", err
	}
	return o.(*Object).url, nil
}

// About gets quota information
func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	return usage, nil
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the SHA-1 of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	err := o.readMetaData(context.TODO())
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return 0
	}
	return o.size
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.Item) (err error) {
	if info.Type != "file" {
		return fmt.Errorf("%q is %q: %w", o.remote, info.Type, fs.ErrorNotAFile)
	}
	o.hasMetaData = true
	o.size = info.Size
	o.modTime = time.Unix(info.CreatedAt, 0)
	o.id = info.ID
	o.mimeType = info.MimeType
	o.url = info.Link
	o.ParentID = info.ParentID
	o.TorrentHash = info.TorrentHash
	o.MappingID = info.MappingID
	o.OriginalLink = info.OriginalLink
	return nil
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) (err error) {
	if o.hasMetaData {
		return nil
	}
	info, err := o.fs.readMetaDataForPath(ctx, o.remote, false, true)
	if err != nil {
		return err
	}
	return o.setMetaData(info)
}

// ModTime returns the modification time of the object
//
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	err := o.readMetaData(ctx)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	if o.url == "" {
		return nil, errors.New("can't download - no URL")
	}
	fs.FixRangeOption(options, o.size)
	var resp *http.Response
	var err_code = 0
	opts := rest.Opts{
		Path:    "",
		RootURL: o.url,
		Method:  "GET",
		Options: options,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		if resp != nil {
			err_code = resp.StatusCode
		}
		if err_code == 503 || err_code == 404 {
			return false, err
		}
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		if err_code == 503 || err_code == 404 {
			for _, TorrentID := range broken_torrents {
				if o.ParentID == TorrentID {
					return nil, err
				}
			}
			err = fmt.Errorf("error opening file: '" + o.url + "' this link seems to be broken - torrent will be re-downloaded")
			broken_torrents = append(broken_torrents, o.ParentID)
		}
		return nil, err
	}
	return resp.Body, err
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	return nil
}

// Remove an object by ID (always a file)
func (f *Fs) remove(ctx context.Context, o *Object) (err error) {
	var resp *http.Response
	var result api.Response
	var oldDirectoryID = ""
	var torrent_files = 0

	// Get parent torrent item
	for i := range torrents {
		if torrents[i].ID == o.ParentID {
			oldDirectoryID = "/" + torrents[i].Name + "/"
			for _, link := range torrents[i].Links {
				if link != "" {
					torrent_files += 1
				}
			}
			break
		}
	}

	// Get all trashed files
	var affected_items []string
	sorting_file.Range(func(key string, value interface{}) bool {
		if strings.Contains(key, oldDirectoryID) {
			if strings.HasSuffix(value.(string), trash_indicator) {
				affected_items = append(affected_items, key)
			}
		}
		return true
	})
	if len(affected_items) == 0 {
		affected_items = append(affected_items, o.MappingID)
	}

	// if not all files are trashed
	if len(affected_items) < torrent_files {
		// move file to trash
		fs.LogPrint(fs.LogLevelDebug, "moving file: "+o.MappingID+" to internal trash")
		f.Move(ctx, o, o.remote+trash_indicator)
		return nil
	}

	// if all files are trashed
	fs.LogPrint(fs.LogLevelDebug, "all files of torrent: "+o.ParentID+" are in internal trash")
	// fs.LogPrint(fs.LogLevelDebug, "remove locking file_mutex")
	file_mutex.Lock()
	defer file_mutex.Unlock()
	// defer fs.LogPrint(fs.LogLevelDebug, "remove unlocking file_mutex")
	// read sort file
	file, err := os.OpenFile(f.opt.SortFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	// delete mappings and torrent if all files are trashed
	scanner := bufio.NewScanner(file)
	var lines []string
	var new_lines = make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return err
	}
	file.Truncate(0)
	file.Seek(0, 0)

	// move all affected items
	for _, affected_item := range affected_items {
		for _, line := range lines {
			if strings.Contains(line, affected_item+move_chars) {
				new_lines[line] = ""
			}
		}
	}
	for _, line := range lines {
		write := line
		if _, ok := new_lines[line]; ok {
			continue
		}
		_, err := file.WriteString(write + "\n")
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	fs.LogPrint(fs.LogLevelDebug, "removing realdebrid torrent id: "+o.ParentID)
	path := "/torrents/delete/" + o.ParentID
	opts := rest.Opts{
		Method:     "DELETE",
		Path:       path,
		Parameters: f.baseParams(),
	}
	err_code := 0
	resp, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
	if resp != nil {
		err_code = resp.StatusCode
	}
	if err_code == 429 {
		time.Sleep(time.Duration(2) * time.Second)
		_, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
	}
	lastcheck = time.Now().Unix() - interval
	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	//fmt.Printf("Removing: '%s'\n", o.remote)
	err := o.readMetaData(ctx)
	if err != nil {
		return fmt.Errorf("Remove: Failed to read metadata: %w", err)
	}
	return o.fs.remove(ctx, o)
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.PublicLinker    = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.MimeTyper       = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
