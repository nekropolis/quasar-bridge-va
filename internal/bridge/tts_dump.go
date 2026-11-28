package bridge

// t547: диагностика громкости TTS. Когда VA_TTS_DUMP=1, OGG/Opus-потоки
// ответов сохраняются в runtime/tts-dump/<stream>_<request>_<route>.ogg,
// чтобы сравнить реальную громкость ответов Алисы (route=yandex) и наших
// (route=llm) через ffmpeg ebur128. По умолчанию выключено, на ответы не влияет.

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
)

const ttsDumpMax = 4 << 20 // 4 МБ на поток — с запасом на длинный ответ

type ttsDumper struct {
	mu    sync.Mutex
	sizes map[uint32]int64
}

func (n *Native) dumpTTSAudio(data []byte) {
	if os.Getenv("VA_TTS_DUMP") != "1" || len(data) < 4 {
		return
	}
	sid := binary.BigEndian.Uint32(data[:4])
	n.mu.Lock()
	p := n.streams[sid]
	if p == nil {
		p = n.cloudStreams[sid]
	}
	if p == nil {
		n.mu.Unlock()
		return
	}
	tag, route := requestTag(p.id), p.route
	n.mu.Unlock()

	if n.dumper == nil {
		n.dumper = &ttsDumper{sizes: map[uint32]int64{}}
	}
	d := n.dumper
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sizes[sid] >= ttsDumpMax {
		return
	}
	dir := filepath.Join("runtime", "tts-dump")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	name := filepath.Join(dir, tag+"_"+route+"_"+itoa(sid)+".ogg")
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	chunk := data[4:]
	written, _ := f.Write(chunk)
	d.sizes[sid] += int64(written)
}

func itoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [10]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
