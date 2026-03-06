package crypto

import (
	"testing"
)

func TestGenerateJupyterPasswordHash(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{
			name: "valid key",
			key:  "my-secret-key",
		},
		{
			name: "empty key",
			key:  "",
		},
		{
			name: "long key",
			key:  "this-is-a-very-long-encryption-key-that-should-work-fine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := GenerateJupyterPasswordHash(tt.key)
			if err != nil {
				t.Errorf("GenerateJupyterPasswordHash() error = %v", err)
			}
			t.Log(h)
		})
	}
}

func TestVerifyJupyterPassword(t *testing.T) {

	tests := []struct {
		name string
		key  string
		hash string
	}{
		{
			name: "valid key",
			key:  "my-secret-key",
			hash: "argon2:$argon2id$v=19$m=10240,t=10,p=8$GIvYy1UJ1cHIp2oz0DyIgA$WaEbuNIa0/b8lb2LhGwlInKdxN9FXLe+too/+D0Rqhw",
		},
		{
			name: "empty key",
			key:  "",
			hash: "argon2:$argon2id$v=19$m=10240,t=10,p=8$UyB0NGf5TCotn1Bfryy2kA$jCZ842Ni4hF1Xa++Md6wAMda5I1reNVb7Ti3XOERwDo",
		},
		{
			name: "long key",
			key:  "this-is-a-very-long-encryption-key-that-should-work-fine",
			hash: "argon2:$argon2id$v=19$m=10240,t=10,p=8$ymaQsOGyHDRg1yamPiqTTg$kvuRN3jH/zYcTBqP3Ebbu9txYrP7HKeA56j+vR4GTuA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encrypt
			//_, err := GenerateJupyterPasswordHash(tt.name)
			//if err != nil {
			//	t.Errorf("GenerateJupyterPasswordHash() error = %v", err)
			//}

			ok, err := VerifyJupyterPassword(tt.key, tt.hash)
			if err != nil {
				t.Errorf("VerifyJupyterPassword() error = %v", err)
			}
			if !ok {
				t.Errorf("VerifyJupyterPassword() = %v, want %v", ok, true)
			}

		})
	}
}
