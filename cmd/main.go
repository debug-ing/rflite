package main

import (
	"fmt"
	"log"
	"rflite/internal/executer"
	"rflite/internal/setup"
	"rflite/internal/store"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	g := gin.Default()
	r, err := setup.SetupLeader()
	if err != nil {
		log.Fatalf("failed to setup leader: %v", err)
	}
	g.POST("/connect", func(c *gin.Context) {
		// add to raft for sync data
	})
	g.GET("/status", func(c *gin.Context) {
		list := store.NewStore().ListDatabases()
		status := gin.H{
			"state":  "master", // master or slave
			"leader": r.Leader(),
		}
		c.JSON(200, gin.H{"status": true, "result": gin.H{
			"databases": list,
			"status":    status,
		}})
	})

	g.POST("/db", func(c *gin.Context) {
		var body struct {
			Name string `json:"name" binding:"required"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(400, gin.H{
				"message": "Failed",
				"error":   err.Error(),
			})
			return
		}
		result, err := store.NewStore().CreateDatabase(body.Name)

		if err != nil {
			c.JSON(400, gin.H{
				"message": "Failed",
				"error":   err.Error(),
			})
			return
		}

		c.JSON(200, gin.H{
			"message": "Success",
			"path":    result,
		})
	})

	g.POST("/db/:name/query", func(c *gin.Context) {
		q := c.PostForm("q")
		name := c.Param("name")
		db := store.NewStore().DatabaseExists(name)
		if !db {
			c.JSON(404, gin.H{"error": "database not found"})
			return
		}
		exec := executer.NewExecuter("./db/" + name + ".db")
		result, err := exec.ExecQuery(q)
		if err != nil {
			log.Printf("SQL Exec error: %v", err)
			c.JSON(500, gin.H{"status": false, "error": err.Error()})
			return
		}
		fmt.Println(len(result))
		c.JSON(201, gin.H{"status": true, "result": result})
	})

	g.POST("/db/:name/exec", func(c *gin.Context) {
		q := c.PostForm("q")
		name := c.Param("name")
		tt := "master"
		if tt != "master" {
			c.JSON(403, gin.H{"status": false, "message": "only master can exec queries"})
			return
		}
		query := []byte(fmt.Sprintf("USE %s; %s", name, q))
		if err := r.Apply(query, 5*time.Second).Error(); err != nil {
			c.JSON(500, gin.H{"status": false, "message": err.Error()})
			return
		}
		c.JSON(201, gin.H{"status": true})
	})

	if err := g.Run(":8001"); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
