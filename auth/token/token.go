package token

import (
	"net/http"

	"golang.org/x/oauth2"
)

type TokenAuth struct {
	TokenSource oauth2.TokenSource
}

func (a *TokenAuth) Authenticate(r *http.Request) error {
	t, err := a.TokenSource.Token()
	if err != nil {
		return err
	}
	t.SetAuthHeader(r)
	return nil
}
