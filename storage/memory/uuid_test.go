package memory

import (
	"testing"

	"github.com/pborman/uuid"
)

func TestTwoWaySerialization(t *testing.T) {
	uuid1 := uuid.NewRandom()
	b64 := UUIDToBase64(uuid1)
	uuid2, err := Base64ToUUID(b64)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(b64), 24; got != want {
		t.Fatalf("UUIDToBase64 returned an invalid size string; got %d, want %d", got, want)
	}
	if got, want := uuid2, uuid1; !uuid.Equal(got, want) {
		t.Fatalf("The marshal/unmarshal via base64 should have returned the same UUID; got %q, want %q", got, want)
	}
}

func TestInvalidDeserialization(t *testing.T) {
	if _, err := Base64ToUUID("bogus entry"); err == nil {
		t.Fatal("Base64ToUUID should have never returned a corrupted UUID")
	}
}
