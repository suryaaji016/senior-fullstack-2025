package main

import (
	"context"
    "crypto/sha1"
    "encoding/hex"
	"encoding/json"
	"log"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type User struct {
	RealName string `json:"real_name"`
	Email string `json:"email"`
	Password string `json:"password"`
}

func sha1Hash(text string) string {
	h := sha1.New()
    h.Write([]byte(text))
    return hex.EncodeToString(h.Sum(nil))
}

func main(){
	rdb  := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	app := fiber.New()

	app.Post("/register",func(c *fiber.Ctx) error {
		type Req struct{
			RealName string `json:"real_name"`
			Email string `json:"email"`
			Password string `json:"password"`
		}
		var req Req 
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).SendString("Invalid Input")
		}
		user :=User{
			RealName:req.RealName,
			Email:req.Email,
			Password:sha1Hash(req.Password),
		}
		jsonUser,_ := json.Marshal(user)

		key := "login_" + req.Email

		rdb.Set(ctx,key,jsonUser,0)

		return c.JSON(fiber.Map{
			"message":"User Registered",
			"key":key,
		})
	})

	app.Post("/login",func(c *fiber.Ctx) error{
		type LoginReq struct{
			Email string `json:"email"`
			Password string `json:"password"`
		}
		var req LoginReq 
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).SendString("Invalid Input")
		}
		key := "login_" + req.Email

		val,err :=rdb.Get(ctx,key).Result()

		if err == redis.Nil {
			return c.Status(404).SendString("User Not Found")
		}else if err != nil {
			return c.Status(500).SendString("Redis error")
		}
		var user User
		json.Unmarshal([]byte(val),&user)

		hashedInput := sha1Hash(req.Password)
		if hashedInput != user.Password {
			return c.Status(401).SendString("Wrong Passsword")
		}

		return c.JSON(fiber.Map{
			"message":"Login Success",
			"real_name":user.RealName,
			"email":user.Email,
		})
	})
	log.Fatal(app.Listen(":3000"))
}
