# Qwick (Static On-Disk ART)

Qwick — это высокопроизводительное KV-хранилище на языке Go, оптимизированное для чтения. Оно использует адаптивное
дерево остатков (ART) для индексации и memory-mapped файлы (mmap) для мгновенного доступа к данным с нулевыми
аллокациями по горячему пути.

### Основные возможности

- **Нулевые аллокации при чтении**: Данные возвращаются в виде срезов `[]byte`, указывающих прямо в отображенный в
  память файл.
- **Быстрая индексация**: Использует ART (Adaptive Radix Tree) для эффективного поиска.
- **Встроенная компрессия**: Поддержка `zstd` и `s2` (snappy) для экономии места на диске.
- **Простота**: Минималистичный API.

### Установка

```bash
go get github.com/globalmac/qwick
```

### Использование

#### 1. Запись (сборка базы)

Для создания базы используется `art.Tree` для индексации в памяти, после чего дерево записывается в бинарный файл.

```go
package main

import (

	"encoding/json"
	"fmt"
	
	"github.com/globalmac/qwick"
)

func main() {
	// Создаем новое дерево
	tree := qwick.New()

	// Вставляем данные
	tree.Insert([]byte("user:1"), []byte("Alice"))
	tree.Insert([]byte("user:2"), []byte("Bob"))
	tree.Insert([]byte("admin:1"), []byte("Charlie"))

	// Сохраняем в файл
	if err := qwick.Build(tree, "users.qwick"); err != nil {
		panic(err)
	}
	fmt.Println("База данных успешно создана!")
	
	// или 1 000 000 тестовых записей

    type User struct {
      ID   uint64   `json:"id"`
      Name string   `json:"name"`
      Tags []string `json:"tags"`
    }

    for i := 1; i <= 1_000_000; i++ {
		
      strID := fmt.Sprintf("%d", i)
  
      user := User{
        ID:   uint64(i),
        Name: "username_" + strID,
        Tags: []string{"go" + strID, "db" + strID, "json" + strID},
      }
  
      bin, _ := json.Marshal(user)
      tree.Insert([]byte(strID), bin)
  
      if i%100_000 == 0 {
        fmt.Printf("Inserted: %d\n", i)
      }
	  
    }

    fmt.Println("База данных 1 000 000 строк - успешно создана!")
	
}
```

#### 2. Чтение данных

Чтение происходит мгновенно через `mmap`. Поддерживается два способа получения значения: `GetRaw` (без аллокаций, возвращает ссылку на данные в mmap) и `Find` (с распаковкой, если база сжата).

```go
package main

import (
	
	"encoding/json"
	"fmt"
	
	"github.com/globalmac/qwick"
)

func main() {
	// Открываем существующую базу
	db, err := qwick.Open("users.qwick")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Быстрое получение сырых данных (без аллокаций)
	if val, ok := db.GetRaw([]byte("user:1")); ok {
		fmt.Printf("GetRaw: %s\n", val)
	}

	// Получение данных с поддержкой декомпрессии (если использовалось сжатие)
	dst := make([]byte, 1024)
	if val, ok, err := db.Find([]byte("user:2"), dst); ok && err == nil {
		fmt.Printf("Find: %s\n", val)
	}
	
	// или с JSON Unmarshal
	
    type User struct {
      ID   uint64   `json:"id"`
      Name string   `json:"name"`
      Tags []string `json:"tags"`
    }
	
    // Получение данных с поддержкой декомпрессии (если использовалось сжатие)
    if val, ok, err := db.Find([]byte("123"), dst); ok && err == nil {
      var user User
      _ = json.Unmarshal(val, &user)
      fmt.Println("Find: ", user.ID, user.Name)
    }
      
}
```

#### 3. Поиск по префиксу

Благодаря ART-индексу, поиск по префиксу выполняется крайне эффективно.
Доступно две версии метода: `PrefixRaw` для получения сырых данных и `Prefix` для получения распакованных данных.

```go
package main

import (
	  "fmt"
	
	  "github.com/globalmac/qwick"
)

func main() {
  db, _ := qwick.Open("users.qwick")
  defer db.Close()

  fmt.Println("Поиск всех пользователей (с распаковкой):")
  dst := make([]byte, 1024)
  db.Prefix([]byte("user:"), dst, func(key, val []byte) bool {
    fmt.Printf(" - %s: %s\n", key, val)
    return true // true для продолжения итерации, false для остановки
  })

  fmt.Println("Поиск всех пользователей (сырые данные, без аллокаций):")
  db.PrefixRaw([]byte("user:"), func(key, val []byte) bool {
    fmt.Printf(" - %s: %d bytes\n", key, len(val))
    return true
  })
}
```

#### 4. Продвинутая сборка (Сжатие)

Вы можете настроить алгоритм сжатия и другие параметры при сборке базы.

```go
package main

import (
	"github.com/globalmac/qwick"
)

func main() {
	tree := qwick.New()
	// ... вставка данных ...

	opts := qwick.BuildOptions{
		Compression: 1,    // 1: Zstd, 2: S2, 3: None
		ZstdLevel:   3,    // Уровень сжатия для Zstd
		SizeCutover: 128,  // Не сжимать значения меньше 128 байт
	}

	qwick.BuildWithOptions(tree, "compressed.qwick", opts)
}
```

#### 5. Дополнительное сжатие + шифрование (S2 + AES-256-CTR + Poly1305)

Для чувствительных и больших БД, Вы можете использовать сжатие S2 + AES-256-CTR + Poly1305

```go
package main

import (
  "crypto/rand"
  "encoding/base64"
  "fmt"
  "log"

  "github.com/globalmac/qwick"
)

func main() {

  key := make([]byte, 32)
  rand.Read(key)
  keyRaw := base64.RawStdEncoding.EncodeToString(key)

  fmt.Println("Ключ:", keyRaw)

  // 1. Шифруем (сжатие s2 + шифрование AES-CTR + аутентификация Poly1305)
  err := qwick.ZipEncrypt("file.qwick.enc", "file.qwick", key)
  if err != nil {
    log.Fatalf("ZipEncrypt ошибка: %v", err)
  }

  keyPlain, _ := base64.RawStdEncoding.DecodeString(keyRaw)

  // 2. Расшифровываем (проверка целостности + дешифрование + декомпрессия)
  err = qwick.UnzipDecrypt("file.qwick", "file.qwick.enc", keyPlain)
  if err != nil {
    log.Fatalf("UnzipDecrypt ошибка: %v", err)
  }

}
```
