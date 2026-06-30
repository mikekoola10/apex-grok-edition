package main

import (
	"os"
)

type Config struct {
	ShopifyAPIKey      string
	ShopifyStoreURL    string
	DeepSeekAPIKey     string
	HuggingFaceAPIKey  string
	AdminKey           string
}

var config Config

func init() {
	config = Config{
		ShopifyAPIKey:     os.Getenv("SHOPIFY_API_KEY"),
		ShopifyStoreURL:    os.Getenv("SHOPIFY_STORE_URL"),
		DeepSeekAPIKey:    os.Getenv("DEEPSEEK_API_KEY"),
		HuggingFaceAPIKey: os.Getenv("HUGGING_FACE_API_KEY"),
		AdminKey:          os.Getenv("ADMIN_KEY"),
	}
}
