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
    | jq -r '.[] | select(((.status // "")|ascii_downcase)=="active" and ((.assetClass // "")|ascii_downcase)=="fii") | .ticker' \
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

  parsed_payload="$(
    printf '%s' "${page_html}" \
      | node -e "const fs=require('fs');const html=fs.readFileSync(0,'utf8');const normalize=(value)=>String(value||'').replace(/<[^>]+>/g,' ').replace(/&nbsp;/gi,' ').replace(/\\u00a0/g,' ').replace(/\\s+/g,' ').trim();const result={};const descriptionSection=(html.match(/<section[^>]*id=[\\\"'][^\\\"']*carbon_fields_fiis_description[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/section>/i)||[])[1]||html;const article=(descriptionSection.match(/<article[^>]*class=[\\\"'][^\\\"']*newsContent__article[^\\\"']*[\\\"'][^>]*>[\\s\\S]*?<\\/article>/i)||descriptionSection.match(/<article[^>]*>[\\s\\S]*?<\\/article>/i)||[])[0]||'';if(article){result.description_html=article;}const dividendsSection=(html.match(/<section[^>]*id=[\\\"'][^\\\"']*carbon_fields_fiis_dividends_resume[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/section>/i)||[])[1]||'';if(dividendsSection){const title=normalize((dividendsSection.match(/<h2[^>]*>([\\s\\S]*?)<\\/h2>/i)||[])[1]||'');const txtContainer=(dividendsSection.match(/<div[^>]*class=[\\\"'][^\\\"']*\\btxt\\b[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/div>/i)||[])[1]||'';const paragraphs=[];for(const paragraphMatch of txtContainer.matchAll(/<p[^>]*>([\\s\\S]*?)<\\/p>/gi)){const text=normalize(paragraphMatch[1]||'');if(text){paragraphs.push(text);}}const headContainer=(dividendsSection.match(/<div[^>]*data-element=[\\\"']head[\\\"'][^>]*>([\\s\\S]*?)<\\/div>\\s*<\\/div>\\s*<\\/div>/i)||[])[1]||dividendsSection;const tableBlocks=[];for(const blockMatch of headContainer.matchAll(/<div[^>]*class=[\\\"'][^\\\"']*yieldChart__table__bloco[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/div>/gi)){const lines=[];for(const lineMatch of String(blockMatch[1]||'').matchAll(/<div[^>]*class=[\\\"'][^\\\"']*table__linha[^\\\"']*[\\\"'][^>]*>([\\s\\S]*?)<\\/div>/gi)){const line=normalize(lineMatch[1]||'');if(line){lines.push(line);}}if(lines.length>0){tableBlocks.push(lines);}}let table=null;if(tableBlocks.length>=3){const periods=tableBlocks[0].slice(1);const returnByUnitLabel=tableBlocks[1][0]||null;const returnByUnit=tableBlocks[1].slice(1);const relativeToQuoteLabel=tableBlocks[2][0]||null;const relativeToQuote=tableBlocks[2].slice(1);const columnsCount=Math.min(periods.length,returnByUnit.length,relativeToQuote.length);if(columnsCount>0){table={periods:periods.slice(0,columnsCount),return_by_unit_label:returnByUnitLabel,return_by_unit:returnByUnit.slice(0,columnsCount),relative_to_quote_label:relativeToQuoteLabel,relative_to_quote:relativeToQuote.slice(0,columnsCount)};}}if(title||paragraphs.length>0||table){result.dividends_resume={title:title||null,paragraphs,table,source:'fundsexplorer'};}}process.stdout.write(JSON.stringify(result));"
  )"

  if [[ -z "${parsed_payload}" || "${parsed_payload}" == "{}" ]]; then
    continue
  fi

  jq --arg ticker "${ticker}" --argjson payload "${parsed_payload}" \
    '.items[$ticker] = $payload' \
    "${TMP_FILE}" > "${TMP_FILE}.next"
  mv "${TMP_FILE}.next" "${TMP_FILE}"
done

mkdir -p "$(dirname "${CACHE_FILE}")"
mv "${TMP_FILE}" "${CACHE_FILE}"
echo "Updated ${CACHE_FILE} with ${ticker_count} active FII tickers."
