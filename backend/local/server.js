// Load env vars before anything else.
// Resolve paths from repository root so startup works regardless of current working directory.
try {
	const path = require('path');
	const dotenv = require('dotenv');
	const repoRoot = path.resolve(__dirname, '../..');
	dotenv.config({ path: path.join(repoRoot, '.env') });
	dotenv.config({ path: path.join(repoRoot, '.env.local'), override: true });
} catch {}

// Force local-safe defaults for development server if values are missing.
process.env.APP_ENV = process.env.APP_ENV || 'local';
process.env.DYNAMODB_ENDPOINT =
	process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000';
process.env.TABLE_NAME = process.env.TABLE_NAME || 'wealth-main';
process.env.AWS_REGION = process.env.AWS_REGION || 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || 'local';
process.env.AWS_SECRET_ACCESS_KEY =
	process.env.AWS_SECRET_ACCESS_KEY || 'local';

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
app.use(express.json({ limit: process.env.API_JSON_LIMIT || '30mb' }));

// Convert Express request -> Lambda event format -> call handler -> return response
app.all('/api/*path', async (req, res) => {
	const originalPath = String(req.originalUrl || req.url || req.path || '');
	const path = originalPath.replace(/^\/api/, '').split('?')[0] || '/';

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
		res.status(500).json({
			error: 'Internal server error',
			message:
				process.env.NODE_ENV === 'production'
					? undefined
					: (err && err.message) || String(err),
		});
	}
});

// Health check
app.get('/health', (req, res) => {
	res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
	console.log(`Invest backend running on http://localhost:${PORT}`);
	console.log(`DynamoDB endpoint: ${process.env.DYNAMODB_ENDPOINT || 'http://localhost:8000'}`);
});
