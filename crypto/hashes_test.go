package crypto

import "testing"

func TestHMACMD5(t *testing.T) {
	key := "76f26d2aca5b866c4bbc74168093429e"
	t.Log(HMACMD5("hds", key))
}
