package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// 定义数据结构，但与第一次实验所定义的有所改变
type Bucket struct {
	Nodes []Peer
}

type Peer struct {
	NodeID  string
	KBucket KBucket
}

type KBucket struct {
	Buckets [160]Bucket
}

type KeyHash []byte

func (kb *KBucket) findNode(key []byte) ([]Peer, bool) {
	index := getBucketIndex(key)
	bucket := kb.Buckets[index]
	for _, peer := range bucket.Nodes {
		if bytes.Equal([]byte(peer.NodeID), key) {
			return []Peer{peer}, true
		}
	}

	var closestPeers []Peer
	for i := 0; i < 2; i++ {
		randomBucket := kb.Buckets[rand.Intn(160)]
		if len(randomBucket.Nodes) > 0 {
			randomNode := randomBucket.Nodes[rand.Intn(len(randomBucket.Nodes))]
			peers, found := kb.findNode([]byte(randomNode.NodeID))
			if found {
				closestPeers = append(closestPeers, peers...)
			}
		}
	}

	return closestPeers, false
}

func (kb *KBucket) insertNode(peer Peer) {
	index := getBucketIndex([]byte(peer.NodeID))
	bucket := &kb.Buckets[index]
	for _, node := range bucket.Nodes {
		if node.NodeID == peer.NodeID {
			return
		}
	}

	if len(bucket.Nodes) < 100 {
		bucket.Nodes = append(bucket.Nodes, peer)
	} else {
		bucket.Nodes = append(bucket.Nodes[1:], peer)
	}
}

func (kb *KBucket) SetValue(key, value []byte) {
	kb.Buckets[getBucketIndex(key)].Nodes = append(kb.Buckets[getBucketIndex(key)].Nodes, Peer{NodeID: string(key), KBucket: KBucket{}})
}

func (kb *KBucket) GetValue(key []byte) []byte {
	for _, bucket := range kb.Buckets {
		for _, peer := range bucket.Nodes {
			if peer.NodeID == string(key) {
				return []byte(peer.NodeID)
			}
		}
	}
	return nil
}

func (peer *Peer) SetValue(key, value []byte) bool {
	if !bytes.Equal(key, hash(value)) {
		return false
	}

	if peer.KBucket.GetValue(key) != nil {
		return true
	}

	peer.KBucket.SetValue(key, value)

	closestPeers := peer.KBucket.findClosestPeers(key, 2)
	for _, closestPeer := range closestPeers {
		closestPeer.SetValue(key, value)
	}

	return true
}

func (peer *Peer) GetValue(key []byte) []byte {
	value := peer.KBucket.GetValue(key)
	if value != nil {
		return value
	}

	closestPeers, _ := peer.KBucket.findNode(key)
	for _, closestPeer := range closestPeers {
		value := closestPeer.GetValue(key)
		if value != nil {
			return value
		}
	}

	return nil
}

func (kb *KBucket) findClosestPeers(key []byte, count int) []Peer {
	closestPeers := make([]Peer, 0, count)

	bucketIndex := getBucketIndex(key)
	bucket := kb.Buckets[bucketIndex]
	closestPeers = append(closestPeers, bucket.Nodes...)

	if len(closestPeers) < count {
		for i := 1; i < count; i++ {
			prevBucketIndex := (bucketIndex - i + 160) % 160
			nextBucketIndex := (bucketIndex + i) % 160
			prevBucket := kb.Buckets[prevBucketIndex]
			nextBucket := kb.Buckets[nextBucketIndex]
			closestPeers = append(closestPeers, prevBucket.Nodes...)
			closestPeers = append(closestPeers, nextBucket.Nodes...)

			if len(closestPeers) >= count {
				break
			}
		}
	}

	return closestPeers[:count]
}

// getBucketIndex，采用哈希值异或运算获取索引
func getBucketIndex(nodeID []byte) int {
	distance := hash(nodeID)
	index := 0
	for i := 0; i < len(distance); i++ {
		// 使用异或运算计算索引
		index ^= int(distance[i])
	}
	// 对索引取模，确保不超过159
	index %= 160
	return index
}

func hash(data []byte) KeyHash {
	hasher := sha1.New()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 初始化100个节点
	peers := make([]Peer, 100)
	for i := 0; i < 100; i++ {
		id := "Peer" + strconv.Itoa(i+1)
		peers[i] = Peer{
			NodeID:  id,
			KBucket: KBucket{},
		}

		for j := 0; j < 100; j++ {
			randomPeerID := "Peer" + strconv.Itoa(rand.Intn(100)+1)
			peer := Peer{
				NodeID:  randomPeerID,
				KBucket: KBucket{},
			}
			peers[i].KBucket.insertNode(peer)
		}
	}

	// 随机生成200个key value
	keys := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(fmt.Sprintf("Value %d", i))
		keys[i] = key

		randomPeer := &peers[rand.Intn(100)]
		randomPeer.SetValue(key, value)
	}

	// 是否能查询key
	for _, key := range randomKeys(keys, 100) {
		randomPeer := &peers[rand.Intn(100)]
		value := randomPeer.GetValue(key)
		if value != nil {
			fmt.Printf("Value for Key %s found in Peer %s: %s\n", string(key), randomPeer.NodeID, string(value))
		} else {
			fmt.Printf("Value for Key %s not found in Peer %s\n", string(key), randomPeer.NodeID)
		}
	}
}

// 随机生成函数randomKeys
func randomKeys(keys [][]byte, count int) [][]byte {
	shuffledKeys := make([][]byte, len(keys))
	copy(shuffledKeys, keys)
	rand.Shuffle(len(shuffledKeys), func(i, j int) {
		shuffledKeys[i], shuffledKeys[j] = shuffledKeys[j], shuffledKeys[i]
	})
	return shuffledKeys[:count]
}
