# climate-mediator
Processes climate-related data as an example of unstructured data handling

## Local Development Setup

### Prerequisites
- Node.js (v14 or higher)
- OpenHIM Core and Console running locally (see [OpenHIM documentation](https://openhim.org/docs/installation/getting-started))

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/jembi/climate-mediator.git
   cd climate-mediator
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Create a .env file in the root directory:
   ```bash
   touch .env
   ```

4. Build the TypeScript code:
   ```bash
   npm run build
   ```

5. Start the development server:
   ```bash
   npm start
   ```

6. The mediator will be running on port 3000 by default. You can override this by setting the `SERVER_PORT` environment variable in your `.env` file:
   ```bash
   SERVER_PORT=3001
   ```

