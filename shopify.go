package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type ShopifyClient struct {
	AccessToken string
	StoreURL    string
}

func NewShopifyClient() *ShopifyClient {
	return &ShopifyClient{
		AccessToken: config.ShopifyAPIKey,
		StoreURL:    config.ShopifyStoreURL,
	}
}

func (c *ShopifyClient) query(query string, variables map[string]interface{}) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://%s/admin/api/2024-01/graphql.json", c.StoreURL)

	payload := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shopify-Access-Token", c.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, nil
}

func (c *ShopifyClient) CreateProduct(title, description, imageURL string) (string, error) {
	query := `
	mutation productCreate($input: ProductInput!) {
	  productCreate(input: $input) {
	    product {
	      id
	      title
	    }
	    userErrors {
	      field
	      message
	    }
	  }
	}`

	variables := map[string]interface{}{
		"input": map[string]interface{}{
			"title":       title,
			"descriptionHtml": description,
			"images": []map[string]string{
				{"src": imageURL},
			},
		},
	}

	res, err := c.query(query, variables)
	if err != nil {
		return "", err
	}

	// Simplified parsing for simulation/example
	return fmt.Sprintf("Product Created: %v", res), nil
}

func (c *ShopifyClient) FetchOrders() (string, error) {
	query := `
	{
	  orders(first: 10) {
	    edges {
	      node {
		id
		name
		totalPriceSet {
		  shopMoney {
		    amount
		    currencyCode
		  }
		}
	      }
	    }
	  }
	}`

	res, err := c.query(query, nil)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Orders Fetched: %v", res), nil
}

func (c *ShopifyClient) UpdateInventory(inventoryItemId string, delta int) error {
	// Implementation for inventory adjustment
	return nil
}

func (c *ShopifyClient) ApplyBundleDiscount(checkoutId string) error {
	// Implementation for applying discount
	return nil
}
