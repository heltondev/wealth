// Load env vars before anything else
try { require('dotenv').config(); } catch {}

const express = require('express');
const cors = require('cors');
const { handler } = require('../handlers/wealth-lambda-handler');

const app = express();
const PORT = process.env.PORT || 3001;

// Mock Cognito claims for local development
const MOCK_CLAIMS = {
	sub: 'local-user-001',
	email: 'oliver@local.dev',
	'cognito:groups': '[ADMIN]',
};

app.use(cors());
app.use(express.json());

// Convert Express request -> Lambda event format -> call handler -> return response
app.all('/api/*path', async (req, res) => {
	const path = req.path.replace('/api', '');

	const event = {
		httpMethod: req.method,
		path,
		headers: {
			...req.headers,
			origin: req.headers.origin || 'http://localhost:5173',
		},
		body: req.body ? JSON.stringify(req.body) : null,
		requestContext: {
			authorizer: {
				claims: MOCK_CLAIMS,
			},
		},
		queryStringParameters: req.query || {},
	};

	try {
		const result = await handler(event);
		res.status(result.statusCode);
		Object.entries(result.headers || {}).forEach(([key, value]) => {
			res.setHeader(key, value);
		});
		res.send(result.body);
	} catch (err) {
		console.error('Handler error:', err);
		res.status(500).json({ error: 'Internal server error' });
	}
});

// Health check
app.get('/health', (req, res) => {
	res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
	console.log(`WealthHub backend running on http://localhost:${PORT}`);
	console.log(`DynamoDB endpoint: ${process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000'}`);
});
