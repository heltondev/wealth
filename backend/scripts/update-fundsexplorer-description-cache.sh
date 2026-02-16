#!/usr/bin/env bash
set -eo pipefail

BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_FILE="${BACKEND_DIR}/data/fundsexplorer-descriptions.json"
TMP_FILE="$(mktemp)"
trap 'rm -f "${TMP_FILE}" "${TMP_FILE}.next" "${ARTICLE_FILE:-}"' EXIT

API_BASE="${API_BASE:-http://localhost:3001}"
PORTFOLIO_ID="${PORTFOLIO_ID:-oliver-main}"

TICKERS_RAW="$(
  curl -s "${API_BASE}/api/portfolios/${PORTFOLIO_ID}/assets" \
    | jq -r '.[] | select((.status|ascii_downcase)=="active" and (.assetClass|ascii_downcase)=="fii") | .ticker' \
    | sort -u
)"

printf '{"updated_at":"%s","items":{}}' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" > "${TMP_FILE}"

ticker_count=0

for ticker in ${TICKERS_RAW}; do
  ticker_count=$((ticker_count + 1))
  slug="$(printf '%s' "${ticker}" | tr '[:upper:]' '[:lower:]')"
  page_html="$(curl -sL "https://www.fundsexplorer.com.br/funds/${slug}" || true)"
  if [[ -z "${page_html}" ]]; then
    continue
  fi

  article_html="$(
    printf '%s' "${page_html}" \
      | node -e "const fs=require('fs');const html=fs.readFileSync(0,'utf8');const section=(html.match(/<section[^>]*id=[\\\"'][^\\\"']*carbon_fields_fiis_description[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/section>/i)||[])[1]||html;const article=(section.match(/<article[^>]*class=[\\\"'][^\\\"']*newsContent__article[^\\\"']*[\\\"'][^>]*>[\\s\\S]*?<\\/article>/i)||section.match(/<article[^>]*>[\\s\\S]*?<\\/article>/i)||[])[0]||'';if(article)process.stdout.write(article);"
  )"

  if [[ -z "${article_html}" ]]; then
    continue
  fi

  ARTICLE_FILE="$(mktemp)"
  printf '%s' "${article_html}" > "${ARTICLE_FILE}"
  jq --arg ticker "${ticker}" --rawfile html "${ARTICLE_FILE}" \
    '.items[$ticker] = {description_html: $html}' \
    "${TMP_FILE}" > "${TMP_FILE}.next"
  mv "${TMP_FILE}.next" "${TMP_FILE}"
  rm -f "${ARTICLE_FILE}"
done

mkdir -p "$(dirname "${CACHE_FILE}")"
mv "${TMP_FILE}" "${CACHE_FILE}"
echo "Updated ${CACHE_FILE} with ${ticker_count} active FII tickers."
