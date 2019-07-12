package gphotos

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/palantir/stacktrace"
	"golang.org/x/oauth2"

	"github.com/gphotosuploader/googlemirror/api/photoslibrary/v1"
	"google.golang.org/api/googleapi"
)

const apiVersion = "v1"
const basePath = "https://photoslibrary.googleapis.com/"

// Client is a client for uploading a media.
// photoslibrary does not provide `/v1/uploads` API so we implement here.
type Client struct {
	*photoslibrary.Service
	*http.Client
	token *oauth2.Token
}

// Token returns the value of the token used by the gphotos Client
// Cannot be used to set the token
func (c *Client) Token() *oauth2.Token {
	if c.token == nil {
		return nil
	}
	return &(*c.token)
}

// NewClient constructs a new PhotosClient from an oauth httpClient
func NewClient(oauthHTTPClient *http.Client, maybeToken ...*oauth2.Token) (*Client, error) {
	var token *oauth2.Token
	switch len(maybeToken) {
	case 0:
	case 1:
		token = maybeToken[0]
	default:
		return nil, stacktrace.NewError("NewClient() parameters should have maximum 1 token")
	}

	photosService, err := photoslibrary.New(oauthHTTPClient)
	if err != nil {
		return nil, err
	}
	return &Client{photosService, oauthHTTPClient, token}, nil
}

// GetUploadToken sends the media and returns the UploadToken.
func (c *Client) GetUploadToken(r io.ReadSeeker, filename, uploadURL string, fileSize int64) (string, string, error) {
	var offset int64

	if uploadURL != "" {
		log.Printf("Checking status of upload URL '%s'\n", uploadURL)
		// Query previous upload status and get offset if active
		req, err := http.NewRequest("POST", uploadURL, nil)
		if err != nil {
			return "", "", err
		}
		req.Header.Set("Content-Length", "0")
		req.Header.Set("X-Goog-Upload-Command", "query")

		res, err := c.Client.Do(req)
		if err != nil {
			return "", "", err
		}
		defer res.Body.Close()

		// Get upload status
		status := res.Header.Get("X-Goog-Upload-Status")
		log.Printf("Status of upload URL '%s' is '%s'\n", uploadURL, status)
		if status == "active" {
			offset, err = strconv.ParseInt(res.Header.Get("X-Goog-Upload-Size-Received"), 10, 0)
			if err == nil && offset > 0 && offset < fileSize {
				r.Seek(offset, io.SeekStart)
			}
		} else {
			// Other known statuses "final" and "cancelled" are both considered an Error by the official Ruby client
			// https://github.com/googleapis/google-api-ruby-client/blob/0.30.2/lib/google/apis/core/upload.rb#L250
			// Let's restart the upload from scratch
			uploadURL = ""
		}
	}

	if uploadURL == "" {
		// Get new upload URL
		log.Printf("Getting new upload URL for '%s'\n", filename)
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/uploads", basePath, apiVersion), nil)
		if err != nil {
			return "", "", err
		}
		req.Header.Set("Content-Length", "0")
		req.Header.Set("X-Goog-Upload-Command", "start")
		req.Header.Add("X-Goog-Upload-Content-Type", "application/octet-stream")
		req.Header.Set("X-Goog-Upload-Protocol", "resumable")
		req.Header.Set("X-Goog-Upload-Raw-Size", fmt.Sprintf("%d", fileSize))

		res, err := c.Client.Do(req)
		if err != nil {
			return "", "", err
		}
		defer res.Body.Close()

		// Read upload url and pass it through the chan to live the opportunity to the caller to store it for resumable uploads
		uploadURL = res.Header.Get("X-Goog-Upload-URL")
	}

	log.Printf("Uploading content to '%s'\n", uploadURL)

	contentLength := int(fileSize - offset)
	req, err := http.NewRequest("POST", uploadURL, &ReadProgressReporter{r: r, max: contentLength, fileSize: int(fileSize)})
	if err != nil {
		log.Printf("Failed to prepare request: Error '%s'\n", err)
		return "", uploadURL, err
	}
	req.Header.Set("Content-Length", fmt.Sprintf("%d", contentLength))
	req.Header.Add("X-Goog-Upload-Command", "upload, finalize")
	req.Header.Set("X-Goog-Upload-Offset", fmt.Sprintf("%d", offset))

	res, err := c.Client.Do(req)
	if err != nil {
		log.Printf("\nFailed to process request '%s'\n", err)
		return "", uploadURL, err
	}
	defer res.Body.Close()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf("Failed to read response '%s'\n", err)
		return "", uploadURL, err
	}
	uploadToken := string(b)
	return uploadToken, uploadURL, nil
}

// UploadFile actually uploads the media and activates it on google photos
func (c *Client) UploadFile(filePath, uploadURL string, pAlbumID ...string) (*photoslibrary.MediaItem, string, error) {
	// validate parameters
	if len(pAlbumID) > 1 {
		return nil, "", stacktrace.NewError("parameters can't include more than one albumID'")
	}
	var albumID string
	if len(pAlbumID) == 1 {
		albumID = pAlbumID[0]
	}

	log.Printf("Uploading %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, "", stacktrace.Propagate(err, "failed opening file %s", filePath)
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return nil, "", stacktrace.Propagate(err, "failed getting file size %s", filePath)
	}
	fileSize := fileStat.Size()

	uploadToken, uploadURL, err := c.GetUploadToken(file, filePath, uploadURL, fileSize)
	if err != nil {
		return nil, uploadURL, stacktrace.Propagate(err, "failed getting uploadToken for %s", filePath)
	}

	retry := true
	retryCount := 0
	for retry != false {
		retry = false
		batchResponse, err := c.MediaItems.BatchCreate(&photoslibrary.BatchCreateMediaItemsRequest{
			AlbumId: albumID,
			NewMediaItems: []*photoslibrary.NewMediaItem{
				{
					Description:     filePath,
					SimpleMediaItem: &photoslibrary.SimpleMediaItem{UploadToken: uploadToken},
				},
			},
		}).Do()
		if err != nil {
			// handle rate limit error by sleeping and retrying
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 429 {
				after, err := strconv.ParseInt(gerr.Header.Get("Retry-After"), 10, 64)
				if err != nil || after == 0 {
					after = 60
				}
				log.Printf("Rate limit reached, sleeping for %d seconds...", after)
				time.Sleep(time.Duration(after) * time.Second)
				retry = true
				continue
			} else if retryCount < 3 {
				log.Printf("Error during upload, sleeping for 10 seconds before retrying...")
				time.Sleep(10 * time.Second)
				retry = true
				retryCount++
				continue
			}
			return nil, uploadURL, stacktrace.Propagate(err, "failed adding media %s", filePath)
		}

		if batchResponse == nil || len(batchResponse.NewMediaItemResults) != 1 {
			return nil, uploadURL, stacktrace.NewError("len(batchResults) should be 1")
		}
		result := batchResponse.NewMediaItemResults[0]
		if result.Status.Message != "OK" && result.Status.Message != "Success" {
			// TODO: We should use a different field like `googleapi.ServerResponse`
			return nil, uploadURL, stacktrace.NewError("status message should be OK/Succecc, found: %s", result.Status.Message)
		}

		log.Printf("%s uploaded successfully as %s", filePath, result.MediaItem.Id)
		return result.MediaItem, "", nil
	}
	return nil, uploadURL, nil
}

func (c *Client) AlbumByName(name string) (album *photoslibrary.Album, found bool, err error) {
	listAlbumsResponse, err := c.Albums.List().Do()
	if err != nil {
		return nil, false, stacktrace.Propagate(err, "failed listing albums")
	}
	for _, album := range listAlbumsResponse.Albums {
		if album.Title == name {
			return album, true, nil
		}
	}
	return nil, false, nil
}

func (c *Client) GetOrCreateAlbumByName(albumName string) (*photoslibrary.Album, error) {
	// validate params
	{
		if albumName == "" {
			return nil, stacktrace.NewError("albumName can't be empty")
		}
	}

	// try to find album by name
	album, found, err := c.AlbumByName(albumName)
	if err != nil {
		return nil, err
	}
	if found && album != nil {
		return c.Albums.Get(album.Id).Do()
	}

	// else create album
	return c.Albums.Create(&photoslibrary.CreateAlbumRequest{
		Album: &photoslibrary.Album{
			Title: albumName,
		},
	}).Do()
}
