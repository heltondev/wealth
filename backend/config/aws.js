const DEFAULT_REGION = 'us-east-1';
const DEFAULT_TABLE_NAME = 'wealth-main';

const isTruthy = (value) =>
	['1', 'true', 'yes', 'on'].includes(String(value || '').toLowerCase());

const isLocalRuntime = () =>
	Boolean(process.env.DYNAMODB_ENDPOINT) ||
	Boolean(process.env.S3_ENDPOINT) ||
	isTruthy(process.env.IS_OFFLINE) ||
	isTruthy(process.env.AWS_SAM_LOCAL);

const resolveRuntimeEnvironment = () =>
	(process.env.APP_ENV || (isLocalRuntime() ? 'local' : 'aws')).toLowerCase();

const resolveAwsRegion = () => process.env.AWS_REGION || DEFAULT_REGION;

const resolveTableName = () => process.env.TABLE_NAME || DEFAULT_TABLE_NAME;

const resolveS3BucketName = () =>
	process.env.S3_BUCKET || `invest-data-${resolveRuntimeEnvironment()}`;

const buildAwsClientConfig = (options = {}) => {
	const service = String(options.service || '').toLowerCase();
	const region = resolveAwsRegion();
	const endpoint = service === 'dynamodb'
		? process.env.DYNAMODB_ENDPOINT
		: service === 's3'
			? process.env.S3_ENDPOINT
			: undefined;

	const config = { region };
	if (endpoint) config.endpoint = endpoint;

	// Local emulators require static credentials even if the values are placeholders.
	if (endpoint || isLocalRuntime()) {
		config.credentials = {
			accessKeyId: process.env.AWS_ACCESS_KEY_ID || 'local',
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'local',
		};
	}

	return config;
};

module.exports = {
	isLocalRuntime,
	resolveRuntimeEnvironment,
	resolveAwsRegion,
	resolveTableName,
	resolveS3BucketName,
	buildAwsClientConfig,
};
