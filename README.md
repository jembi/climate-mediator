# climate-mediator
Processes climate-related data as an example of unstructured data handling

## Local Development Setup

### Prerequisites

- Node.js (v14 or higher)
- OpenHIM Core and Console running locally (see [OpenHIM documentation](https://openhim.org/docs/installation/getting-started))
- Minio (RELEASE.2022-10-24T18-35-07Z)
- Clickhouse (v23.8.14.6)

### Installation

### 1. Clone the repository

``` bash
git clone https://github.com/jembi/climate-mediator.git
cd climate-mediator
```

### 2. Install dependencies

``` bash
npm install
```

### 3. Create a .env file and tmp folder in the root directory

``` bash
touch .env

```

### 4. Build the TypeScript code

```bash
npm run build
```

### 5. Start the development server

```bash
npm start
```

### 6. The mediator will be running on port 3000 by default. You can override this by setting the `SERVER_PORT` environment variable in your `.env` file:

``` bash
SERVER_PORT=3001
```

### 7. provide all other connection string and credentials for clickhouse and minio

## Minio Configurations

### 1. Login to the Minio Admin panel

``` text
http://localhost:9001

# Default credentials
username: minioadmin
password: minioadmin
```

### 2. Create a minio access key and secret from the "Access Keys" page and set them to the ENVIRONMENT variables

``` bash
MINIO_ACCESS_KEY:
MINIO_SECRETE_KEY:
```

### 3. Set the MINIO_BUCKET

``` bash
MINIO_BUCKET=climate-mediator,
```
