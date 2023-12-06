package strings

import "strings"

const (
	// VersionBig 版本号大
	VersionBig = 1
	// VersionSmall 版本号小
	VersionSmall = -1
	// VersionEqual 版本号相等
	VersionEqual = 0
)

// CompareVersion
// 版本号比较 1: verA > verB; -1:verA < verB; 0:verA == verB
func CompareVersion(verA, verB string) int {
	verStrArrA := splitStrByNet(verA)
	verStrArrB := splitStrByNet(verB)
	return compareArrStrVer(verStrArrA, verStrArrB)
}

// compareArrStrVer 比较版本号字符串数组
func compareArrStrVer(verA, verB []string) int {

	for index, _ := range verA {
		littleResult := compareLittleVer(verA[index], verB[index])
		if littleResult != VersionEqual {
			return littleResult
		}
	}
	return VersionEqual
}

// compareLittleVer 比较小版本号字符串
func compareLittleVer(verA, verB string) int {

	bytesA := []byte(verA)
	bytesB := []byte(verB)

	lenA := len(bytesA)
	lenB := len(bytesB)
	if lenA > lenB {
		return VersionBig
	}

	if lenA < lenB {
		return VersionSmall
	}
	//如果长度相等则按byte位进行比较
	return compareByBytes(bytesA, bytesB)
}

// compareByBytes 按byte位进行比较小版本号
func compareByBytes(verA, verB []byte) int {
	for index, _ := range verA {
		if verA[index] > verB[index] {
			return VersionBig
		}
		if verA[index] < verB[index] {
			return VersionSmall
		}
	}
	return VersionEqual
}

// splitStrByNet 按“.”分割版本号为小版本号的字符串数组
func splitStrByNet(strV string) []string {
	return strings.Split(strV, ".")
}
