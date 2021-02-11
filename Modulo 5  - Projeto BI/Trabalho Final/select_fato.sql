select
	fatura.id_fatura,
	coalesce(cliente.id_cliente, -1) as id_cliente,
	coalesce(produto.id_produto, -1) as id_produto,
	coalesce(vendedor.id_vendedor, -1) as id_vendedor,
	loja.id_loja,
	tempo.id_tempo,
	nullif(fvt.ift_seqitm, '99999')::int as cd_seq_item,
	fvt.vl_venda,
	fvt.vl_custo,
	fvt.vl_lucro,
	fvt.qt_item
from
	fato_venda_temp fvt
left join dim_loja loja
	on (fvt.ift_empresa = loja.cd_empresa and
		fvt.ift_loja = loja.cd_loja)
left join dim_fatura fatura
	on (fvt.ift_empresa = fatura.cd_empresa and
		fvt.ift_loja = fatura.cd_loja and
		fvt.ift_numfat = fatura.cd_fatura)
left join dim_produto produto
	on (fvt.ift_item = produto.cd_produto)
left join dim_vendedor vendedor
	on (fvt.fat_codven = vendedor.cd_vendedor)
left join dim_cliente cliente	
	on (fvt.fat_codpes = cliente.cd_cliente)
left join dim_tempo tempo
	on (fvt.fat_datfat = tempo.dt_data)
;

INSERT INTO fato_venda (id_fatura, id_cliente, id_produto, id_vendedor, id_loja, id_tempo, vl_venda, vl_custo, vl_lucro, qt_item)
select
	fatura.id_fatura,
	cliente.id_cliente,
	produto.id_produto,
	vendedor.id_vendedor,
	loja.id_loja,
	tempo.id_tempo,
	fvt.vl_venda,
	fvt.vl_custo,
	fvt.vl_lucro,
	fvt.qt_item
from
	fato_venda_temp fvt
left join dim_loja loja
	on (fvt.ift_empresa = loja.cd_empresa and
		fvt.ift_loja = loja.cd_loja)
left join dim_fatura fatura
	on (fvt.ift_empresa = fatura.cd_empresa and
		fvt.ift_loja = fatura.cd_loja and
		fvt.ift_numfat = fatura.cd_fatura)
left join dim_produto produto
	on (fvt.ift_item = produto.cd_produto)
left join dim_vendedor vendedor
	on (fvt.fat_codven = vendedor.cd_vendedor)
left join dim_cliente cliente	
	on (fvt.fat_codpes = cliente.cd_cliente)
left join dim_tempo tempo
	on (fvt.fat_datfat = tempo.dt_data)