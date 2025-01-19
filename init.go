package main

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func initDB() (*gorm.DB, error) {
	dsn := "host=localhost user=postgres password=postgres dbname=db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return db, nil
}
