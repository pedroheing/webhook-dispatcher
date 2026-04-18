package env

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

func Load[T any]() (*T, error) {
	_ = godotenv.Load()
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("falha ao carregar configurações: %w", err)
	}
	return &cfg, nil
}
