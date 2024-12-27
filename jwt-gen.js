const jwt = require('jsonwebtoken');
const secret = process.env['JWT_K_PEM']
  if (secret === undefined) {
    throw new Error(
      'Environment variable JWT_K_PEM is not set',
    )
  }
const payload = {
    iss: 'ClickHouse',
    sub: 'CI_Test',
    aud: '1f7f78b8-da67-480b-8913-726fdd31d2fc',
    'clickhouse:roles': ['default'],
    'clickhouse:grants': [],
};

const signed = jwt.sign(payload, secret, { expiresIn: '15m', algorithm: 'RS256' });

console.log(signed);