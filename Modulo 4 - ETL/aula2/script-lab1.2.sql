/* Script lab1.2 */

CREATE TABLE dimensao_ano_mes (
  ano smallint
  ,mes smallint
  ,valor DOUBLE PRECISION
  ,quantidade BIGINT
);

CREATE TABLE dimensao_loja_vendedor (
  vendedorid BIGINT
  ,vendedor VARCHAR(255)
  ,lojaid BIGINT
  ,loja VARCHAR(255)
  ,quantidade BIGINT
  ,valor DOUBLE PRECISION
);

CREATE TABLE dimensao_categoria (
  categoria varchar(255)
  ,id BIGINT
  ,valor DOUBLE PRECISION
);

