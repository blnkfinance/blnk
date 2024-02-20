# Welcome to Blnk

Blnk is your gateway to building powerful financial products with ease. This open-source financial ledger server is the backbone for creating scalable solutions in banking, digital wallets, card processing, and brokerage systems. Let's get you started on deploying Blnk and exploring its capabilities.

## Status at a Glance

Before we dive in, here's how our project is doing:

- **Build and Test**: ![Build and Test Status](https://github.com/jerry-enebeli/Blnk/actions/workflows/go.yml/badge.svg)
- **Deploy to Docker**: ![Deploy to Docker Status](https://github.com/jerry-enebeli/Blnk/actions/workflows/docker-publish.yml/badge.svg)
- **Code Quality (Linter)**: ![Linter Status](https://github.com/jerry-enebeli/Blnk/actions/workflows/lint.yml/badge.svg)

## Why Blnk?

Whether you're launching a fintech startup or integrating financial functionalities into existing services, Blnk offers a robust platform that caters to:

- **Banking Systems**: Streamline operations with our ledger's support.
- **Digital Wallets**: Create wallets that are both flexible and secure.
- **Card Processing Services**: Process payments smoothly and efficiently.
- **Brokerage Systems**: Manage investments with a reliable back-end.

## Getting Started with Blnk

### Docker Deployment

Get Blnk up and running with minimal setup using Docker:

```bash
docker run -v $(pwd)/blnk.json:/blnk.json -p 5001:5001 jerryenebeli/blnk:latest
```

This command ensures your configuration file is used by the Blnk server, making deployment a breeze.

### Building from Source

For those who prefer a hands-on approach or need customizations:

1. Ensure you have Go (version 1.16 or newer): Visit [Go's official site](https://golang.org/doc/install) for installation guides.
2. Clone and build Blnk:

```bash
git clone https://github.com/jerry-enebeli/Blnk && cd Blnk
make build
```

### Configuration Essentials

#### The `blnk.json` Configuration File

To tailor Blnk to your needs, create a `blnk.json` with the following structure:

```json
{
  "project_name": "Blnk",
  "time_zone": "UTC",
  "data_source": {
    "dns": "postgres://username:password@host:port/database?sslmode=disable"
  },
  "redis": {
    "dns": "redis://host:port"
  },
  "account_number_generation": {
    "enable_auto_generation": true,
    "http_service": {
      "url": "http://your-service/mocked-account",
      "headers": {
        "Authorization": "Bearer your_auth_token"
      }
    }
  },
  "server": {
    "domain": "yourdomain.com",
    "ssl": false,
    "ssl_email": "your@email.com",
    "port": "5001"
  },
  "notification": {
    "slack": {
      "webhook_url": "http://your-slack-webhook.com"
    },
    "webhook": {
      "url": "http://your-webhook-url.com",
      "headers": {}
    }
  }
}

```

Ensure you replace placeholders (e.g., `username`, `password`) with your actual database credentials.

#### Environment Variables

If you prefer using environment variables or if `blnk.json` is unavailable, here's what Blnk looks for:

| Variable                | Description                                       | Default Value |
|-------------------------|---------------------------------------------------|---------------|
| `BLNK_SERVER_PORT`      | Server port number.                               | `5001`        |
| `BLNK_SERVER_SSL_EMAIL` | Email for SSL certificate.                        |               |
| `BLNK_SERVER_SSL_DOMAIN`| Domain for SSL certificate.                       |               |
| `BLNK_SERVER_SSL`       | Enable SSL (`true` or `false`).                   | `false`       |
| `BLNK_PROJECT_NAME`     | Name of the project.                              |               |
| `BLNK_REDIS_DNS`        | Redis server DNS.                                 |               |
| `BLNK_DATA_SOURCE_DNS`  | Database server DNS.                              |               |

### Supported Databases

For now, Blnk proudly supports PostgreSQL:

- **PostgreSQL**: ✅

---

## Dive Deeper into Blnk
To learn more about Blnk and its features in detail, visit our comprehensive documentation on the [Blnk Wiki](https://github.com/jerry-enebeli/blnk/wiki).

---

