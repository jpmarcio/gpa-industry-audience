from meudesconto import MeuDesconto
obj = MeuDesconto()

audience = {"modalidade":{"tipotarget":"PLU","produto":["0252447"],"vendor_class_code":"5697"}, "filtros":{"genero":["F","M"], "idade":[1, 2, 3, 4, 5, 6], "lealdadeProduto":6, "sensibilidadePreco":"0", "operador":"AND"} }

ret_audiencia  = obj.calcular_audiencia_industria(audience)


audience = {
    "id": 252454,
    "produtos": [
      "0252454"
    ],
    "precoProduto": 7.79,
    "filtros": {
      "genero": [
        "M",
        "F"
      ],
      "idade": [
        1,
        2,
        3,
        4,
        5,
        6
      ],
      "lealdadeProduto": 5,
      "sensibilidadePreco": "0",
      "operador": "AND",
      "un": [],
      "dna": []
    },
    "regioes": [
      "SP",
      "RJ",
      "DF",
      "GO",
      "MG",
      "TO"
    ],
    "mecanica": "",
    "unidadesMaxProduto": "",
    "grupo": "519",
    "categoria": "100",
    "subcategoria": "109"
  }


audience={"modalidade":{"tipotarget":"PLU","produto":["0252454"],"departamento":1,"bandeira":"EX","familia":"5007","vendor_class_code":"5697","vendor_code":"0000000000394"},"precoProduto":"7.79","pFidelidade":"EX","filtros":{"genero":[],"idade":[],"lealdadeProduto":5,"sensibilidadePreco":"","operador":"OR","operadorDna":"","un":[{"codigo":1019614048,"descricao":"CAFE SOLUVEL"},{"codigo":1019610102001,"descricao":"FEIJAO CARIOCA"},{"codigo":1019610027002,"descricao":"ARROZ BRANCO DE 2.1KG A 5KG"}],"dna":[]},"regioes":[],"mecanica":"","unidadesMaxProduto":"","duracao":""}

  
audience={"modalidade":{"tipotarget":"PLU","produto":["0252454"],"departamento":1,"bandeira":"EX","familia":"5007","vendor_class_code":"5697","vendor_code":"0000000000394"},"precoProduto":"7.79","pFidelidade":"EX","filtros":{"genero":[],"idade":[],"lealdadeProduto":20,"sensibilidadePreco":"","operador":"OR","operadorDna":"","un":[{"codigo":1019614048,"descricao":"CAFE SOLUVEL"},{"codigo":1019610102001,"descricao":"FEIJAO CARIOCA"}],"dna":[]},"regioes":[],"mecanica":"","unidadesMaxProduto":"","duracao":""}