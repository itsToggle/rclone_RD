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
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

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

//Global variables
var cached []api.Item
var torrents []api.Item
var broken_torrents []string
var lastcheck int64 = time.Now().Unix()
var lastFileMod int64 = 0
var interval int64 = 15 * 60
var mapping = make(map[string]string)
var sorting_file = make(map[string]string)
var folders = make(map[string][]api.Item)
var regex_folders = make(map[string]string)
var regex_defs = make(map[string]*regexp.Regexp)
var move_chars = " -> "
var regx_chars = " == "
var default_sorting = `# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~ rclone_rd sorting file ~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# - write comment lines using "#"
#
# - write regex definitions using: folder + " == " + regex definition. You can edit the exising ones or create new ones.
#   Order matters for regex folders, first match will be final destination. Make sure there are no trailing space characters.
#   torrents that dont match any regex definition end up in a folder named "default".
#   Example: /movies == (?i)(19|20)([0-9]{2} ?\.?)
#
# - create new directories using "/foldername"
#   Example: /shit
#
# - write move/renaming changes using: "actual torrent title" + "/" + "actual file name" + " -> " + "destination"
#   You do not need to create the directories you are moving stuff to, this will be done automatically.
#   Example: /Our.Universe.S01.1080p.[rartv] -> /shows/Our Universe/Season 1

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
			Help:     `please provide the full path to a file that should be used for sorting`,
			Advanced: true,
			Default:  "./sorting.txt",
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
	fs          *Fs       // what this object is part of
	remote      string    // The remote path
	hasMetaData bool      // metadata is present and correct
	size        int64     // size of the object
	modTime     time.Time // modification time of the object
	id          string    // ID of the object
	ParentID    string    // ID of parent directory
	mimeType    string    // Mime type of object
	url         string    // URL to download file
	TorrentHash string    // Torrent Hash
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
	fmt.Println("Redownloading dead torrent: " + torrent.Name)
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
				resp, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
				for resp.StatusCode == 429 && retries <= 5 {
					time.Sleep(time.Duration(2) * time.Second)
					resp, _ = f.srv.CallJSON(ctx, &opts, nil, &result)
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
	if _, ok := folders[dirID]; ok {
		return true
	}
	return false
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

	if _, ok := folders[dirID]; !ok {
		if _, ok := folders[dirID+"/"]; ok {
			dirID = dirID + "/"
		} else if _, ok := folders["/"+dirID+"/"]; ok {
			dirID = "/" + dirID + "/"
		} else if len(dirID) > 1 {
			if last := dirID[len(dirID)-1]; last == '/' {
				if _, ok := folders[dirID[:len(dirID)-1]]; ok {
					dirID = dirID[:len(dirID)-1]
				}
			}
		}
	}
	// Open the sorting file

	file, err := os.Open(f.opt.SortFile)
	if os.IsNotExist(err) {
		fs.LogPrint(fs.LogLevelNotice, "Creating sorting file.")
		file, err = os.OpenFile(f.opt.SortFile, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		if _, err := file.WriteString(default_sorting); err != nil {
			fmt.Println(err)
		}
	} else if err != nil {
		fmt.Println(err)
	} else {
		defer file.Close()
	}

	var updated = false
	fileInfo, _ := file.Stat()
	fileModTime := fileInfo.ModTime().Unix()

	if fileModTime > lastFileMod {
		updated = true
	}

	if (dirID == rootID) || !(f.folder_exists(dirID)) || updated {

		// Create folder structure
		//
		if updated {
			// Reset saved folder structure
			fs.LogPrint(fs.LogLevelInfo, "Reading updated sorting file.")
			folders = make(map[string][]api.Item)
			regex_folders = make(map[string]string)
			regex_defs = make(map[string]*regexp.Regexp)
			mapping = make(map[string]string)
			sorting_file = make(map[string]string)
			// Flush the directory cache
			f.dirCache.Flush()
			// Read the file line by line
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "#") {
					continue
				} else if len(scanner.Text()) == 0 || scanner.Text() == "\n" || scanner.Text() == "\n\r" {
					continue
				} else if strings.Contains(scanner.Text(), move_chars) {
					// Split the line by " -> "
					parts := strings.Split(scanner.Text(), move_chars)
					// Add the key-value pair to the map
					mapping[parts[0]] = parts[1]
				} else if strings.Contains(scanner.Text(), regx_chars) {
					// Split the line by " -> "
					parts := strings.Split(scanner.Text(), regx_chars)
					// Add the key-value pair to the map
					regex_folders[parts[0]] = parts[1]
				} else {
					mapping[scanner.Text()] = scanner.Text()
				}
			}

			//create regex definitions
			for folder, regex_def := range regex_folders {
				r, _ := regexp.Compile(regex_def)
				regex_defs[folder] = r
			}

			for key, value := range mapping {
				sorting_file[key] = value
			}

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

		totalcount = 2
		for len(newcached) < totalcount {
			partialresult = nil
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
			var retries = 0
			for resp.StatusCode == 429 && retries <= 5 {
				partialresult = nil
				time.Sleep(time.Duration(2) * time.Second)
				resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
				retries += 1
			}
			if err == nil {
				totalcount, err = strconv.Atoi(resp.Header["X-Total-Count"][0])
				if err == nil {
					if totalcount != len(cached) || time.Now().Unix()-lastcheck > interval {
						if time.Now().Unix()-lastcheck > interval && !printed {
							fs.LogPrint(fs.LogLevelInfo, "Updating all links and torrents.")
							printed = true
						}
						newcached = append(newcached, partialresult...)
						opts.Parameters.Set("offset", strconv.Itoa(len(newcached)))
						opts.Parameters.Set("limit", "2500")
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
		for len(newtorrents) < totalcount {
			partialresult = nil
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
			var retries = 0
			for resp.StatusCode == 429 && retries <= 5 {
				partialresult = nil
				time.Sleep(time.Duration(2) * time.Second)
				resp, err = f.srv.CallJSON(ctx, &opts, nil, &partialresult)
				retries += 1
			}
			if err == nil {
				totalcount, err = strconv.Atoi(resp.Header["X-Total-Count"][0])
				if err == nil {
					if totalcount != len(torrents) || time.Now().Unix()-lastcheck > interval {
						newtorrents = append(newtorrents, partialresult...)
						opts.Parameters.Set("offset", strconv.Itoa(len(newtorrents)))
						opts.Parameters.Set("limit", "2500")
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
		if updated {
			for i := range torrents {
				//handle dead torrents
				broken = false
				for _, TorrentID := range broken_torrents {
					if torrents[i].ID == TorrentID {
						broken = true
					}
				}
				if torrents[i].Status == "dead" || broken {
					torrents[i] = f.redownloadTorrent(ctx, torrents[i])
				}
				//set default torrents[i] location
				torrents[i].DefaultLocation = "/default/"
				for folder, r := range regex_defs {
					match := r.MatchString(torrents[i].Name)
					if match {
						torrents[i].DefaultLocation = folder + "/"
						break
					}
				}
				//iterate through files
				var broken = false
				for _, link := range torrents[i].Links {
					if link == "" {
						continue
					}
					var ItemFile api.Item
					for _, cachedfile := range cached {
						if cachedfile.OriginalLink == link {
							ItemFile = cachedfile
							break
						}
					}
					if ItemFile.Name == "" {
						continue
					}
					if ItemFile.Link == "" {
						fs.LogPrint(fs.LogLevelDebug, "Creating new unrestricted direct link for torrent: "+torrents[i].Name)
						path = "/unrestrict/link"
						method = "POST"
						opts := rest.Opts{
							Method: method,
							Path:   path,
							MultipartParams: url.Values{
								"link": {link},
							},
							Parameters: f.baseParams(),
						}
						resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
						if resp.StatusCode == 503 {
							broken = true
							break
						}
						var retries = 0
						for resp.StatusCode == 429 && retries <= 5 {
							time.Sleep(time.Duration(2) * time.Second)
							resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
							retries += 1
						}
					}
					mapping_id := "/" + torrents[i].Name + "/" + ItemFile.Name
					ItemFile.DefaultLocation = torrents[i].DefaultLocation + torrents[i].Name + "/"
					ItemFile.ParentID = torrents[i].ID
					ItemFile.TorrentHash = torrents[i].TorrentHash
					ItemFile.Generated = torrents[i].Generated
					ItemFile.Type = "file"
					if _, ok := mapping[mapping_id]; !ok {
						if _, ok := mapping["/"+torrents[i].Name+"/"]; ok {
							mapping[mapping_id] = mapping["/"+torrents[i].Name+"/"]
						} else {
							mapping[mapping_id] = ItemFile.DefaultLocation
						}
					} else {
						ItemFile.Name = strings.Split(mapping[mapping_id], "/")[len(strings.Split(mapping[mapping_id], "/"))-1]
						if ItemFile.Name == "" {
							continue
						}
						mapping[mapping_id] = strings.Join(strings.Split(mapping[mapping_id], "/")[:len(strings.Split(mapping[mapping_id], "/"))-1], "/") + "/"
					}
					folders[mapping[mapping_id]] = append(folders[mapping[mapping_id]], ItemFile)
				}
				if broken {
					torrents[i] = f.redownloadTorrent(ctx, torrents[i])
					for _, link := range torrents[i].Links {
						var ItemFile api.Item
						fs.LogPrint(fs.LogLevelDebug, "Creating new unrestricted direct link for torrent: "+torrents[i].Name)
						path = "/unrestrict/link"
						method = "POST"
						opts := rest.Opts{
							Method: method,
							Path:   path,
							MultipartParams: url.Values{
								"link": {link},
							},
							Parameters: f.baseParams(),
						}
						resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
						var retries = 0
						for resp.StatusCode == 429 && retries <= 5 {
							time.Sleep(time.Duration(2) * time.Second)
							resp, _ = f.srv.CallJSON(ctx, &opts, nil, &ItemFile)
							retries += 1
						}
						if ItemFile.Name == "" {
							continue
						}
						mapping_id := "/" + torrents[i].Name + "/" + ItemFile.Name
						ItemFile.DefaultLocation = torrents[i].DefaultLocation + torrents[i].Name + "/"
						ItemFile.ParentID = torrents[i].ID
						ItemFile.TorrentHash = torrents[i].TorrentHash
						ItemFile.Generated = torrents[i].Generated
						ItemFile.Type = "file"
						if _, ok := mapping[mapping_id]; !ok {
							if _, ok := mapping["/"+torrents[i].Name+"/"]; ok {
								mapping[mapping_id] = mapping["/"+torrents[i].Name+"/"]
							} else {
								mapping[mapping_id] = ItemFile.DefaultLocation
							}
						} else {
							ItemFile.Name = strings.Split(mapping[mapping_id], "/")[len(strings.Split(mapping[mapping_id], "/"))-1]
							if ItemFile.Name == "" {
								continue
							}
							mapping[mapping_id] = strings.Join(strings.Split(mapping[mapping_id], "/")[:len(strings.Split(mapping[mapping_id], "/"))-1], "/") + "/"
						}
						folders[mapping[mapping_id]] = append(folders[mapping[mapping_id]], ItemFile)
					}
				}
			}

			// Iterate through the map
			for _, newLocation := range mapping {

				// Split the new location by "/"
				locationParts := strings.Split(newLocation, "/")

				// Create a new variable to store the full path
				var location string

				// Iterate through each level of the new location
				for _, locationPart := range locationParts {
					// Append the full path to the corresponding level
					var skip bool
					skip = false
					for _, val := range folders[location] {
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
						folders[location] = append(folders[location], ItemFolder)
					}
					// Append the current location part to the full path
					location = location + locationPart + "/"
				}
			}

		}

	}

	result = append(result, folders[dirID]...)

	if err != nil {
		return newDirID, found, fmt.Errorf("couldn't list files: %w", err)
	}
	for i := range result {
		item := &result[i]
		layout := "2006-01-02T15:04:05.000Z"
		if item.Generated != "" {
			t, _ := time.Parse(layout, item.Generated)
			item.CreatedAt = t.Unix()
		} else if item.Ended != "" {
			t, _ := time.Parse(layout, item.Ended)
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
	//fmt.Printf("Purging torrent: '%s'\n", rootID)
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	dc := f.dirCache
	rootID, err := dc.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	path := "/torrents/delete/" + rootID
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
	for _, torrent := range torrents {
		if torrent.ID == oldDirectoryID {
			oldDirectoryID = "/" + torrent.Name + "/"
			break
		} else if !isFile && torrent.Name == oldLeaf {
			oldDirectoryID = "/"
			break
		}
	}
	// Handle Files
	if isFile && len(id) == 13 {
		for _, file := range cached {
			if file.ID == id {
				oldLeaf = file.Name
				break
			}
		}
	}
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
	if !isFile {
		if last := newLeaf[len(newLeaf)-1]; last != '/' {
			newLeaf = newLeaf + "/"
		}
		if last := oldLeaf[len(oldLeaf)-1]; last != '/' {
			oldLeaf = oldLeaf + "/"
		}
	}
	// Get all files that should be moved.
	var affected_items []string
	for key, value := range sorting_file {
		if strings.Contains(value, oldDirectoryID+oldLeaf) {
			affected_items = append(affected_items, key)
		}
	}
	if len(affected_items) == 0 {
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
				new_dst := strings.Replace(strings.Split(line, move_chars)[len(strings.Split(line, move_chars))-1], oldDirectoryID+oldLeaf, newDirectoryID+newLeaf, -1)
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
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	// Create temporary object
	dstObj, leaf, directoryID, err := f.createObject(ctx, remote, srcObj.modTime, srcObj.size)
	if err != nil {
		return nil, err
	}

	// Do the move
	err = f.move(ctx, true, srcObj.id, path.Base(srcObj.remote), leaf, srcObj.ParentID, directoryID)
	if err != nil {
		return nil, err
	}

	err = dstObj.readMetaData(ctx)
	if err != nil {
		return nil, err
	}
	return dstObj, nil
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
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		if err_code == 503 || err_code == 404 {
			for _, TorrentID := range broken_torrents {
				if o.ParentID == TorrentID {
					return nil, err
				}
			}
			err = fmt.Errorf("Error opening file: '" + o.url + "'. This link seems to be broken. Torrent will be re-downloaded on next refresh.")
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

// Remove an object by ID
func (f *Fs) remove(ctx context.Context, id ...string) (err error) {
	//fmt.Printf("Removing direct link id: '%s'\n", id[0])
	//if f.opt.RootFolderID == "torrents" {
	//	fmt.Printf("Removing torrent id: '%s'\n", id[1])
	//}
	path := "/downloads/delete/" + id[0]
	opts := rest.Opts{
		Method:     "DELETE",
		Path:       path,
		Parameters: f.baseParams(),
	}
	var resp *http.Response
	var result api.Response
	var retries = 0
	err_code := 0
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
	path = "/torrents/delete/" + id[1]
	opts = rest.Opts{
		Method:     "DELETE",
		Path:       path,
		Parameters: f.baseParams(),
	}
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
	if o.ParentID != "" {
		return o.fs.remove(ctx, o.id, o.ParentID)
	} else {
		return o.fs.remove(ctx, o.id)
	}
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
