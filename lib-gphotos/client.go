package gphotos

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/palantir/stacktrace"
	"golang.org/x/oauth2"

	"google.golang.org/api/googleapi"
	photoslibrary "google.golang.org/api/photoslibrary/v1"
)

const apiVersion = "v1"
const basePath = "https://photoslibrary.googleapis.com/"

// PhotosClient is a client for uploading a media.
// photoslibrary does not provide `/v1/uploads` API so we implement here.
type Client struct {
	*photoslibrary.Service
	*http.Client
	token *oauth2.Token
}

func parse429Header(header http.Header) int64 {
	after, err := strconv.ParseInt(header.Get("Retry-After"), 10, 32)
	if err != nil {
		return 0
	}
	return after
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	var err error
	for ; attempts > 0; attempts-- {
		err = fn()
		if err != nil {
			switch err.(type) {
			case stopStatus:
				// fn() returned critical stop
				return err.(stopStatus).error
			case retryStatus:
				errRetry := err.(retryStatus)
				if errRetry.retryAfter != 0 {
					time.Sleep(time.Duration(errRetry.retryAfter) * time.Second)
				} else {
					time.Sleep(sleep)
				}
				// exponential backoff
				sleep *= 2
				continue
			default:
				// fn() returned unknown err
				return err
			}
		}
		// fn() was ok return nil no retry
		return nil
	}
	// return the final error
	return err
}

type stopStatus struct {
	error
}

type retryStatus struct {
	error
	// retry after seconds
	retryAfter int
}

// Token returns the value of the token used by the gphotos Client
// Cannot be used to set the token
func (c *Client) Token() *oauth2.Token {
	if c.token == nil {
		return nil
	}
	return &(*c.token)
}

// New constructs a new PhotosClient from an oauth httpClient
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
func (client *Client) GetUploadToken(r *os.File, filename string) (token string, err error) {
	// NoopCloser prevents body from closing so we can retry
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s/uploads", basePath, apiVersion), ioutil.NopCloser(r))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Add("X-Goog-Upload-File-Name", filename)
	req.Header.Set("X-Goog-Upload-Protocol", "raw")

	// start retry
	var res *http.Response
	retryErr := retry(3, 1, func() error {
		r.Seek(0, 0)
		res, err = client.Client.Do(req)
		if err != nil {
			// internal error just stop
			return stopStatus{error: err}
		}
		if res == nil {
			return stopStatus{error: errors.New("empty response")}
		}
		if res.StatusCode != 200 {
			switch res.StatusCode {
			case 429:
				after := parse429Header(res.Header)
				log.Printf("429 throttle waiting %d sec", after)
				return retryStatus{retryAfter: int(after)}

			default:
				// for now we'll just quit. in future we can retry other errors
				return nil
			}
		}
		// we're ok res will have response body
		return nil
	})
	if retryErr != nil {
		return "", retryErr
	}
	// end retry
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Upload actually uploads the media and activates it on google photos
func (client *Client) UploadFile(filePath string, pAlbumID ...string) (*photoslibrary.MediaItem, error) {
	// validate parameters
	if len(pAlbumID) > 1 {
		return nil, stacktrace.NewError("parameters can't include more than one albumID'")
	}
	var albumID string
	if len(pAlbumID) == 1 {
		albumID = pAlbumID[0]
	}

	filename := path.Base(filePath)
	log.Printf("Uploading %s", filename)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed opening file")
	}
	defer file.Close()

	uploadToken, err := client.GetUploadToken(file, filename)
	if err != nil {
		return nil, stacktrace.Propagate(err, "failed getting uploadToken for %s", filename)
	}

	var batchResponse *photoslibrary.BatchCreateMediaItemsResponse
	retryErr := retry(3, 1, func() error {
		batchResponse, err = client.MediaItems.BatchCreate(&photoslibrary.BatchCreateMediaItemsRequest{
			AlbumId: albumID,
			NewMediaItems: []*photoslibrary.NewMediaItem{
				&photoslibrary.NewMediaItem{
					Description:     filename,
					SimpleMediaItem: &photoslibrary.SimpleMediaItem{UploadToken: uploadToken},
				},
			},
		}).Do()
		if err != nil {
			// handle rate limit error by sleeping and retrying
			if err.(*googleapi.Error).Code == 429 {
				after := parse429Header(err.(*googleapi.Error).Header)
				log.Printf("Rate limit reached, sleeping for %d seconds...", after)
				return retryStatus{retryAfter: int(after), error: err}
			}
			log.Printf("Unknown error uploading will retry")
			return retryStatus{error: err}
		}
		return nil
	})
	if retryErr != nil {
		return nil, stacktrace.Propagate(err, "failed adding media %s", filename)
	}

	if batchResponse == nil || len(batchResponse.NewMediaItemResults) != 1 {
		return nil, stacktrace.NewError("len(batchResults) should be 1")
	}
	result := batchResponse.NewMediaItemResults[0]
	if result.Status.Message != "OK" {
		return nil, stacktrace.NewError("status message should be OK, found: %s", result.Status.Message)
	}

	log.Printf("%s uploaded successfully as %s", filename, result.MediaItem.Id)
	return result.MediaItem, nil
}

func (client *Client) AlbumByName(name string) (album *photoslibrary.Album, found bool, err error) {
	listAlbumsResponse, err := client.Albums.List().Do()
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

func (client *Client) GetOrCreateAlbumByName(albumName string) (*photoslibrary.Album, error) {
	// validate params
	{
		if albumName == "" {
			return nil, stacktrace.NewError("albumName can't be empty")
		}
	}

	// try to find album by name
	album, found, err := client.AlbumByName(albumName)
	if err != nil {
		return nil, err
	}
	if found && album != nil {
		return client.Albums.Get(album.Id).Do()
	}

	// else create album
	return client.Albums.Create(&photoslibrary.CreateAlbumRequest{
		Album: &photoslibrary.Album{
			Title: albumName,
		},
	}).Do()
}

// func (client *Client) UpsertAlbum(album photoslibrary.Album) (*photoslibrary.Album, error) {
// 	if album.Id != "" {
// 		getAlbumResponse, err := client.Albums.Get(album.Id).Fields()
// 		if err != nil {
// 			return nil, err
// 		}
// 		getAlbumResponse.Album
// 	}
// }
