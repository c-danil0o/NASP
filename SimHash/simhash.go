package simhash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
)

func parseText(text string) map[string]int {
	occurences := make(map[string]int)
	var temp string
	for _, word := range strings.Split(text, " ") {
		if word[len(word)-1:] == "," || word[len(word)-1:] == "." || word[len(word)-1:] == "!" || word[len(word)-1:] == "?" || word[len(word)-1:] == ":" || word[len(word)-1:] == ";" {
			temp = strings.ToLower(word[0 : len(word)-1])

		} else {
			temp = strings.ToLower(word)
		}

		//fmt.Println(temp)
		if _, ok := occurences[temp]; ok {
			occurences[temp] += 1
		} else {
			occurences[temp] = 1
		}

	}
	return occurences
}

func CalculateHash(data map[string]int) []byte {
	out := make([]int, 256)
	for key, value := range data {
		hashstr := ToBinary(GetMD5Hash(key))
		for i, bit := range hashstr {
			if bit == '1' {
				out[i] += value
			} else {
				out[i] += -1 * value
			}
		}
	}
	outbyte := make([]byte, 256)
	for i, v := range out {
		if v > 0 {
			outbyte[i] = 1
		} else {
			outbyte[i] = 0
		}
	}
	return outbyte
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

//func main() {
//
//	var simhash1 []byte
//	var simhash2 []byte
//
//	if input, err := os.ReadFile("tekst1.txt"); err == nil {
//		str := string(input)
//		parsedtxt := parseText(str)
//		simhash1 = CalculateHash(parsedtxt)
//	}
//	if input, err := os.ReadFile("tekst2.txt"); err == nil {
//		str := string(input)
//		parsedtxt := parseText(str)
//		simhash2 = CalculateHash(parsedtxt)
//	}
//
//	c := make([]byte, 256)
//	for i := range simhash1 {
//		c[i] = simhash1[i] ^ simhash2[i]
//	}
//	fmt.Printf("%d", c)
//}
