/* Script lab1.1 */

CREATE TABLE locacao (
  id BIGINT
  ,filme VARCHAR(255)
  ,genero VARCHAR(255)
  ,datalocacao TIMESTAMP
  ,vendedorid BIGINT
  ,valor double precision
);

CREATE TABLE loja (
  id BIGINT
  ,loja VARCHAR(255)
);

CREATE TABLE vendedor (
  id BIGINT
  ,vendedor VARCHAR(255)
  ,lojaid BIGINT
);

