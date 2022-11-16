from cmath import inf
import json
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os


def clearConsole(): return os.system('cls'
                                     if os.name in ('nt', 'dos') else 'clear')


print(
    """----------------------------
| Connecting in Cassandra  |
----------------------------""")


cloud_config = {
    'secure_connect_bundle': 'secure-connect-fatec.zip'
}
auth_provider = PlainTextAuthProvider(
    'NfAqqzHjWApSzZbxUgKZCWiu',
    'wq-vcZiJJD75UCiMqi5qLpzNtqZMFuliq9y+Ugkzr5iB,,gZ9_hgMeZ,qvyYQzy_rp7v_t7HHtkft8LEqdSRcegosuBS6-.kN71+nL+mRHLuE783MX6U1.+8d2ZZorUY')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

session.execute("USE nosql;")

print("Generete all tables in project...")
session.execute(
    "CREATE TABLE IF NOT EXISTS usuario (email text PRIMARY KEY, nome text, cpf text, end text, fav text);")
print("User created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS vendedor (email text PRIMARY KEY, nome text, cpf text, end text);")
print("Vendedor created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS produtos (id text PRIMARY KEY, nome text, preco text, vendedor text);")
print("Produtos created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS compra (id text PRIMARY KEY, usuario text, produto text, total text);")
print("Successful!")

# Ajustes


def AjusteEndereco(end):
    splitEndereco = end.split(",")
    createDicEndereco = {'rua': ' ', 'bairro': ' ',
                         'cep': ' ', 'estado': ' ', 'cidade': ' '}
    createDicEndereco.update({'rua': splitEndereco[0], 'bairro': splitEndereco[1],
                             'cep': splitEndereco[2], 'estado': splitEndereco[3], 'cidade': splitEndereco[4]})
    return json.dumps(createDicEndereco)


def ajusteUserVendedor(alvo):
    info = {'email': ' ', 'nome': ' ',
            'cpf': ' ', 'end': ' '}
    info.update({'email': alvo.email, 'nome': alvo.nome,
                 'cpf': alvo.cpf, 'end': alvo.end})
    return json.dumps(info)


def ajusteProduto(alvo):
    info = {'id': '', 'nome': '', 'preco': ''}
    info.update({'id': alvo.id, 'nome': alvo.nome, 'preco': alvo.preco})
    return json.dumps(info)


# Insert
def insertUser(email, nome, cpf, end):
    end = AjusteEndereco(end)
    insert = session.prepare(
        "INSERT INTO usuario( email, nome, cpf, end) values (?, ?, ?, ?)")
    session.execute(insert, [email, nome, cpf, end])


def insertFavoritoUser():
    alvo = input("Qual é o email?: ")
    usuarios = session.execute(
        "SELECT * FROM usuario"
    )
    produtosJaNoFav = []
    for usuario in usuarios:
        if usuario.email == alvo:
            produtos = session.execute("SELECT * FROM produtos")
            for prod in produtos:
                print(f'produto id: {prod.id} | produto nome: {prod.nome}')
            teste = json.loads(usuario.fav)
            for itens in teste:
                itens = json.loads(itens)
                produtosJaNoFav.append(itens)
            produtos = session.execute("SELECT * FROM produtos")
            produto = input("Digite o id: ")
            for prod in produtos:
                if prod.id == produto:
                    produto = ajusteProduto(prod)
                    produtosJaNoFav.append(produto)
            session.execute(
                "UPDATE usuario SET fav = '{favorito}' where email = '{alvo}'".format(
                    favorito=json.dumps(produtosJaNoFav), alvo=alvo)
            )


def insertVendedor(email, nome, cpf, end):
    end = AjusteEndereco(end)
    insert = session.prepare(
        "INSERT INTO vendedor( email, nome, cpf, end) values (?, ?, ?, ?)")
    session.execute(insert, [email, nome, cpf, end])


def insertProduto(preco, nome):
    insert = session.prepare(
        "INSERT INTO produtos( id, preco, nome, vendedor) values (?, ?, ?, ?)")
    id = str(random.randint(1, 10000))
    vendedores = session.execute("SELECT * FROM vendedor;")
    for vendedor in vendedores:
        print(f"Email dos vendedores: {vendedor.email}")
    mercante = input("Digite o email do vendedor")
    if vendedor.email == mercante:
        mercante = ajusteUserVendedor(vendedor)
        session.execute(insert, [id, nome, preco, mercante])


def insertCompras(usuario):
    usuarios = session.execute("SELECT * FROM usuario")
    insert = session.prepare(
        "INSERT INTO compra( id, usuario, produto, total) values (?, ?, ?, ?)")
    produtosAdicional = []
    valorTotal = 0
    for user in usuarios:
        if user.email == usuario:
            usuario = ajusteUserVendedor(user)
            on = True
            produtos = session.execute("SELECT * FROM produtos")
            for prod in produtos:
                while on:
                    option = input("Adicionar produto? Sim/Nao: ")
                    if option == 'Sim':
                        produtos = session.execute("SELECT * FROM produtos")
                        produto = input("Digite o id: ")
                        for prod in produtos:
                            if prod.id == produto:
                                produto = ajusteProduto(prod)
                                valorTotal += int(prod.preco)
                                produtosAdicional.append(produto)
                    elif option == 'Nao':
                        id = str(random.randint(1, 1000))
                        session.execute(
                            insert, [id, usuario, json.dumps(produtosAdicional), str(valorTotal)])
                        on = False
                    elif not option or not option == "Sim" or not option == "Nao":
                        print("Digite uma opção valida...\n")


# Updates


def updateUser():
    inputavel = input("Opção desejada: Nome | cpf | end | Fav")
    opcao = ["nome", "cpf"]
    if inputavel in opcao:
        alvo = input("Quem você deseja alterar? ")
        novo_valor = input()
        session.execute(
            "UPDATE usuario set {mudar} = '{novo_valor}' where email = '{alvo}'".format(mudar=inputavel, novo_valor=novo_valor, alvo=alvo))
    elif inputavel == 'end':
        alvo = input("Quem você deseja alterar? ")
        endereco_novo = input(
            "Digite o endereço no formato: Rua, Bairro, Cep, Estado, Cidade: ")
        session.execute(
            "UPDATE usuario set end = '{novo_valor}' where email = '{alvo}'".format(novo_valor=AjusteEndereco(endereco_novo), alvo=alvo))
    elif inputavel == 'email':
        print('Email é a chave primaria, por favor tente outro valor')


def updateProduto():
    inputavel = input("Opção desejada: nome | preco ")
    alvo = input("Qual produto você deseja alterar? Id:  ")
    opcao = ["nome", "preco"]
    if inputavel in opcao:
        novo_valor = input("Digite o novo valor: ")
        session.execute(
            "UPDATE produtos set {mudar} = '{novo_valor}' where id = '{alvo}'".format(mudar=inputavel, novo_valor=novo_valor, alvo=alvo))


def updateVendedor():
    inputavel = input("Opção desejada: Nome | cpf | end ")
    opcao = ["nome", "cpf"]
    if inputavel in opcao:
        alvo = input("Quem você deseja alterar? ")
        novo_valor = input()
        session.execute(
            "UPDATE vendedor set {mudar} = '{novo_valor}' where email = '{alvo}'".format(mudar=inputavel, novo_valor=novo_valor, alvo=alvo))
    elif inputavel == 'end':
        alvo = input("Quem você deseja alterar? ")
        endereco_novo = input(
            "Digite o endereço no formato: Rua, Bairro, Cep, Estado, Cidade")
        session.execute(
            "UPDATE vendedor set end = '{novo_valor}' where email = '{alvo}'".format(novo_valor=AjusteEndereco(endereco_novo), alvo=alvo))
    elif inputavel == 'email':
        print('Email é a chave primaria, por favor tente outro valor')

# Delete


def deleteUser(alvo):
    delete = session.prepare(
        "DELETE FROM usuario WHERE email = ?")
    session.execute(delete, [alvo])
    print("Excluido com sucesso")


def deleteProduto(alvo):
    delete = session.prepare(
        "DELETE FROM produtos WHERE id = ?")
    session.execute(delete, [alvo])
    print("Excluido com sucesso")


def deleteVendedor(alvo):
    delete = session.prepare(
        "DELETE FROM vendedor WHERE id = ?")
    session.execute(delete, [alvo])
    print("Excluido com sucesso")


def deleteCompra(alvo):
    delete = session.prepare(
        "DELETE FROM compra WHERE id = ?")
    session.execute(delete, [alvo])
    print("Excluido com sucesso")

# FindOneQuery


def findOneUser(alvo):
    select = session.prepare(
        "SELECT * FROM usuario WHERE email = ?")
    select_end = session.prepare(
        "SELECT end FROM usuario WHERE email = ?"
    )
    select_fav = session.prepare(
        "SELECT fav FROM usuario WHERE email = ?"
    )
    execute = session.execute(select, [alvo])
    execute_end = session.execute(select_end, [alvo])
    execute_fav = session.execute(select_fav, [alvo])
    for user in execute:
        print(
            f"""\nInformação Basicas\nNome: {user.nome}\nEmail: {user.email}\nCPF: {user.cpf}""")
    for end in execute_end:
        json_loads = json.loads(end.end)
        print(
            f"""\nEndereço\nRua:{json_loads["rua"]}\nBairro:{json_loads["bairro"]}\nCep: {json_loads["cep"]}\nEstado:{json_loads["estado"]}\nCidade: {json_loads["cidade"]}""")
    for fav in execute_fav:
        json_loads = json.loads(fav.fav)
        print("\nItens no Favorito")
        for favorito in json_loads:
            favorito = json.loads(favorito)
            print("Id: {id}, nome: {nome}, preoço: {preco}".format(
                id=favorito['id'], nome=favorito['nome'], preco=favorito['preco']))


def findOneProduto(alvo):
    select = session.prepare(
        "SELECT * FROM produtos WHERE id = ?")
    execute = session.execute(select, [alvo])
    for prod in execute:
        print(
            f"""\nID: {prod.id}\nNome: {prod.nome}\nPreço: {prod.preco}\nVendedor: {prod.vendedor}""")


def findOneVendedor(alvo):
    select = session.prepare(
        "SELECT * FROM vendedor WHERE email = ?")
    select_end = session.prepare(
        "SELECT end FROM vendedor WHERE email = ?"
    )
    execute = session.execute(select, [alvo])
    execute_end = session.execute(select_end, [alvo])
    for user in execute:
        print(
            f"""\nNome: {user.nome}\nEmail: {user.email}\nCPF: {user.cpf}\nEndereço: {user.end}""")
        for end in execute_end:
            json_loads = json.loads(end.end)
            print(
                f"""\nEndereço\nRua:{json_loads["rua"]}\nBairro:{json_loads["bairro"]}\nCep: {json_loads["cep"]}\nEstado:{json_loads["estado"]}\nCidade: {json_loads["cidade"]}""")


def findOneCompra(alvo):
    select = session.prepare(
        "SELECT * FROM compra WHERE id = ?")
    execute = session.execute(select, [alvo])
    for compra in execute:
        json_loads_usuario = json.loads(compra.usuario)
        print("\nInformação do usuario")
        print(
            f'''Email: {json_loads_usuario['email']}\nNome: {json_loads_usuario['nome']}\nCPF: {json_loads_usuario['cpf']}\n''')
        json_loads_endereco = json.loads(json_loads_usuario['end'])
        json_loads_produtos = json.loads(compra.produto)
        index = 1
        for produtos in json_loads_produtos:
            produtos = json.loads(produtos)
            print('Produto {index}\nId: {id}\nNome: {nome}\nPreço: {preco}\n'.format(
                index=index, nome=produtos['nome'], preco=produtos['preco'], id=produtos['id']))
            index = index + 1
        print("Local de entrega")
        print(
            f"""Endereço\nRua:{json_loads_endereco["rua"]}\nBairro:{json_loads_endereco["bairro"]}\nCep: {json_loads_endereco["cep"]}\nEstado:{json_loads_endereco["estado"]}\nCidade: {json_loads_endereco["cidade"]}""")


# FindManyQuery


def findManyUser():
    usuarios = session.execute(
        "SELECT * FROM usuario"
    )
    index = 1
    for usuario in usuarios:
        print("Usuario {index}\nNome: {nome}\nEmail: {email}\nCpf: {cpf}\n".format(
            index=index, nome=usuario.nome, email=usuario.email, cpf=usuario.cpf
        ))
        index += 1


def findManyProdutos():
    produtos = session.execute(
        "SELECT * FROM produtos"
    )
    for produto in produtos:
        print("\nInformação Basica do produto")
        print("Id: {id}\nNome: {nome}\nPreço: {preco}".format(
            id=produto.id, nome=produto.nome, preco=produto.preco))
        json_loads_vendedor = json.loads(produto.vendedor)
        print("\nVendedor")
        print("Nome: {nome}\nEmail: {email}\n".format(
            nome=json_loads_vendedor['nome'], email=json_loads_vendedor['email']))


def findManyVendedor():
    vendedores = session.execute(
        "SELECT * FROM vendedor"
    )
    index = 1
    for vendedor in vendedores:
        print("Usuario {index}\nNome: {nome}\nEmail: {email}\nCpf: {cpf}\n".format(
            index=index, nome=vendedor.nome, email=vendedor.email, cpf=vendedor.cpf
        ))
        index += 1


def findManyCompra():
    compras = session.execute(
        "SELECT * FROM compra")
    for compra in compras:
        print("Id da compra: " + compra.id)
        json_loads_usuario = json.loads(compra.usuario)
        print("Usuario")
        print("Nome: {nome}\nEmail: {email}".format(
            nome=json_loads_usuario['nome'], email=json_loads_usuario['email']
        ))
        prepare_loads = json.loads(compra.produto)
        print("Itens comprados")
        for itens in prepare_loads:
            produtos_loads = json.loads(itens)
            print("Nome: {nome} | Preço: {preco}".format(
                nome=produtos_loads['nome'], preco=produtos_loads['preco']
            ))


def main():
    clearConsole()
    on = True
    while on:
        print(
            """
|   [1] - Insert de Usuario
|   [2] - Insert Favorito no usuario
|   [3] - Insert Vendedor
|   [4] - Insert Produto
|   [5] - Insert Compras
|   [6] - Update User
|   [7] - Update Produto
|   [8] - Update Vendedor
|   [9] - Delete Usuario
|   [10] - Delete Produto
|   [11] - Delete Vendedor
|   [12] - Delete Compra
|   [13] - Achar um Usuario
|   [14] - Achar um Produto
|   [15] - Achar um Vendedor
|   [16] - Achar uma Compra
|   [17] - Listar Usuarios
|   [18] - Listar Produtos
|   [19] - Listar Vendedores
|   [20] - Listar Compras
|   [X] - Sair
|   [Z] - Limpar console
"""
        )
        opcao = input("Digite a opção: ")
        if opcao == '1':
            email = input("Digite o email: ")
            nome = input("Digite o nome: ")
            cpf = input("Digite o Cpf: ")
            print("Digite o endereço no formato: Rua, Bairro, Cep, Estado, Cidade ")
            end = input("...")
            insertUser(email=email, nome=nome, cpf=cpf, end=end)
        elif opcao == '2':
            insertFavoritoUser()
        elif opcao == '3':
            email = input("Digite o email: ")
            nome = input("Digite o nome: ")
            cpf = input("Digite o Cpf: ")
            print("Digite o endereço no formato: Rua, Bairro, Cep, Estado, Cidade ")
            end = input("...")
            insertVendedor(email=email, nome=nome, cpf=cpf, end=end)
        elif opcao == '4':
            nome = input("Digite o nome do produto: ")
            preco = input("Ditite o preço do produto: ")
            insertProduto(preco=preco, nome=nome)
        elif opcao == '5':
            email = input("Digite o email do usuario: ")
            insertCompras(usuario=email)
        elif opcao == '6':
            updateUser()
        elif opcao == '7':
            updateProduto()
        elif opcao == '8':
            updateVendedor()
        elif opcao == '9':
            alvo = input("Digite o email do alvo: ")
            deleteUser(alvo=alvo)
        elif opcao == '10':
            alvo = input("Digite o id do alvo: ")
            deleteProduto(alvo=alvo)
        elif opcao == '11':
            alvo = input("Digite o email do alvo: ")
            deleteVendedor(alvo=alvo)
        elif opcao == '12':
            alvo = input("Digite o id do alvo: ")
            deleteCompra(alvo=alvo)
        elif opcao == '13':
            alvo = input("Digite o email do alvo: ")
            findOneUser(alvo=alvo)
        elif opcao == '14':
            alvo = input("Digite o id do alvo: ")
            findOneProduto(alvo=alvo)
        elif opcao == '15':
            alvo = input("Digite o email do alvo: ")
            findOneVendedor(alvo=alvo)
        elif opcao == '16':
            alvo = input("Digite o id do alvo: ")
            findOneCompra(alvo=alvo)
        elif opcao == '17':
            findManyUser()
        elif opcao == '18':
            findManyProdutos()
        elif opcao == '19':
            findManyVendedor()
        elif opcao == '20':
            findManyCompra()
        elif opcao == 'X' or opcao == 'x':
            on = False
        elif opcao == 'z' or opcao == 'Z':
            clearConsole()
            return main()
        else:
            clearConsole()
            print("Opção não entendida, Porfavor tente novamente")


main()
