package crypto

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// 生成Jupyter格式的Argon2id哈希密码
// password: 明文密码（如"11111"）
// 返回值: Jupyter可识别的哈希字符串（如 argon2:$argon2id$v=19$m=10240,t=10,p=8$xxx$yyy）
func GenerateJupyterPasswordHash(password string) (string, error) {
	// Jupyter默认的Argon2参数（和notebook.auth.passwd()保持一致）
	const (
		memory      = 10240 // 内存开销 (KB)
		iterations  = 10    // 迭代次数
		parallelism = 8     // 并行度
		saltLength  = 16    // 盐值长度
		keyLength   = 32    // 哈希结果长度
	)

	// 1. 生成随机盐值
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("生成盐值失败: %v", err)
	}

	// 2. 使用Argon2id生成哈希
	hash := argon2.IDKey(
		[]byte(password),
		salt,
		iterations,
		memory,
		uint8(parallelism),
		keyLength,
	)

	// 3. 按Jupyter格式拼接（argon2:$argon2id$v=19$m=xxx,t=xxx,p=xxx$盐值$哈希值）
	// 注意：v=19是Argon2的版本号，固定值
	saltBase64 := base64.RawStdEncoding.EncodeToString(salt)
	hashBase64 := base64.RawStdEncoding.EncodeToString(hash)

	params := fmt.Sprintf("m=%d,t=%d,p=%d", memory, iterations, parallelism)
	hashStr := fmt.Sprintf("argon2:$argon2id$v=19$%s$%s$%s", params, saltBase64, hashBase64)

	return hashStr, nil
}

// 验证密码（可选：用于测试生成的哈希是否正确）
func VerifyJupyterPassword(password, hashStr string) (bool, error) {
	// 1. Parse the encoded hash string
	parts := strings.Split(hashStr, "$")
	if len(parts) != 6 {
		return false, fmt.Errorf("invalid hash format")
	}

	if parts[1] != "argon2id" {
		return false, fmt.Errorf("incompatible variant")
	}

	var version int
	_, err := fmt.Sscanf(parts[2], "v=%d", &version)
	if err != nil {
		return false, err
	}
	if version != argon2.Version {
		return false, fmt.Errorf("incompatible version")
	}

	var memory, iterations uint32
	var parallelism uint8
	_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism)
	if err != nil {
		return false, err
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}

	decodedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}

	// 2. Compute the hash of the provided password using the same parameters
	keyLength := uint32(len(decodedHash))
	otherHash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, keyLength)

	// 3. Compare the computed hash with the decoded hash (Constant Time Compare)
	if subtle.ConstantTimeCompare(decodedHash, otherHash) == 1 {
		return true, nil
	}
	return false, nil
}
