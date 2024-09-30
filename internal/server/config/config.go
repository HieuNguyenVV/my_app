package config

type Config struct {
	Connection struct {
		Kafka struct {
			Hosts   string
			Message struct {
				Topic string
				Group string
			}
		}
		Postgresql struct {
			DB       string
			Host     string
			User     string
			Password string
		}
		Redis struct {
			IsCluster bool
			Host      string
			Password  string
		}
	}
}
