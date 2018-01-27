package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/pkg/errors"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/search"
	"google.golang.org/appengine/urlfetch"
)

func init() {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/batch/import-tweets", importHandler)
	http.Handle("/", r)
}

func importHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	if err := importTweets(ctx); err != nil {
		log.Errorf(ctx, "error %+v" err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func importTweets(ctx context.Context) error {
	log.Infof(ctx, "Start")
	tweets, err := fetch(ctx)
	if err != nil {
		return err
	}

	log.Infof(ctx, "Feched %d tweets", len(tweets))
	if err := put(ctx, tweets); err != nil {
		return err
	}

	log.Infof(ctx, "Completed!!")
	return nil
}

// DueDate ...
type DueDate time.Time

// UnmarshalJSON ...
func (d *DueDate) UnmarshalJSON(raw []byte) error {
	s := strings.Trim(string(raw), `"`)
	s = strings.Replace(s, "/", "-", -1)
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, s)
	if err != nil {
		return errors.WithStack(err)
	}
	*d = DueDate(t)
	return nil
}

// Tweet ...
type Tweet struct {
	ID           string  `json:"id"`
	CreatedAt    DueDate `json:"created_datetime"`
	UserName     string  `json:"user.screen_name"`
	Text         string  `json:"text"`
	RetweetCount int     `json:"retweet_count"`
	MediaURL     *string `json:"media_urls"`
}

func fetch(ctx context.Context) ([]Tweet, error) {
	const urlTmpl = "https://raw.githubusercontent.com/t-analyzers/t-analyzers.github.io/master/data/tweets_%s.json"
	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")
	url := fmt.Sprintf(urlTmpl, yesterday)
	resp, err := urlfetch.Client(ctx).Get(url)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()

	var tweets []Tweet
	if err := json.NewDecoder(resp.Body).Decode(&tweets); err != nil {
		return nil, errors.WithStack(err)
	}
	return tweets, nil
}

func put(ctx context.Context, tweets []Tweet) error {
	type Data struct {
		ID           string
		CreatedAt    time.Time
		UserName     string
		Text         string
		RetweetCount float64
		MediaURL     string
	}

	index, err := search.Open("tweets")
	if err != nil {
		return errors.WithStack(err)
	}

	const limit = 200
	for tmp := range chunks(tweets, limit) {
		var ids []string
		var srcs []interface{}
		for _, t := range tmp {
			ids = append(ids, t.ID)
			data := &Data{
				ID:           t.ID,
				CreatedAt:    time.Time(t.CreatedAt),
				UserName:     t.UserName,
				Text:         t.Text,
				RetweetCount: float64(t.RetweetCount),
			}
			if t.MediaURL != nil {
				data.MediaURL = *t.MediaURL
			}
			srcs = append(srcs, data)
		}
		if _, err := index.PutMulti(ctx, ids, srcs); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func chunks(l []Tweet, n int) chan []Tweet {
	ch := make(chan []Tweet)
	go func() {
		for i := 0; i < len(l); i += n {
			from := i
			to := i + n
			if to > len(l) {
				to = len(l)
			}
			ch <- l[from:to]
		}
		close(ch)
	}()
	return ch
}
