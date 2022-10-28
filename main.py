from cmath import inf
import json
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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
    return createDicEndereco


def ajusteUserVendedor(alvo):
    info = {'email': ' ', 'nome': ' ',
            'cpf': ' ', 'end': ' '}
    info.update({'email': alvo.email, 'nome': alvo.nome,
                 'cpf': alvo.cpf, 'end': alvo.end})
    return info


def ajusteProduto(alvo):
    info = {'id': '', 'nome': '', 'preco': ''}
    info.update({'id': alvo.id, 'nome': alvo.nome, 'preco': alvo.preco})
    return info


# Insert
def insertUser(email, nome, cpf, end):
    end = str(AjusteEndereco(end))
    insert = session.prepare(
        "INSERT INTO usuario( email, nome, cpf, end) values (?, ?, ?, ?)")
    session.execute(insert, [email, nome, cpf, end])


def insertVendedor(email, nome, cpf, end):
    end = str(AjusteEndereco(end))
    insert = session.prepare(
        "INSERT INTO vendedor( email, nome, cpf, end) values (?, ?, ?, ?)")
    session.execute(insert, [email, nome, cpf, end])


def insertProduto(preco, nome, mercante):
    insert = session.prepare(
        "INSERT INTO produtos( id, preco, nome, vendedor) values (?, ?, ?, ?)")
    id = str(random.randint(1, 1000))
    vendedores = session.execute("SELECT * FROM vendedor;")
    for vendedor in vendedores:
        if vendedor.email == mercante:
            mercante = str(ajusteUserVendedor(vendedor))
            session.execute(insert, [id, nome, preco, mercante])


def insertCompras(usuario, produto):
    usuarios = session.execute("SELECT * FROM usuario")
    produtos = session.execute("SELECT * FROM produtos")
    insert = session.prepare(
        "INSERT INTO compra( id, usuario, produto, total) values (?, ?, ?, ?)")
    produtosAdicional = []
    valorTotal = 0
    for user in usuarios:
        if user.email == usuario:
            usuario = str(ajusteUserVendedor(user))
            on = True
            for prod in produtos:
                if prod.id == produto:
                    produto = str(ajusteProduto(prod))
                    valorTotal += int(prod.preco)
                    produtosAdicional.append(produto)
                while on:
                    option = input("Adicionar produto? Sim/Nao")
                    if option == 'Sim':
                        produto = input("Outro id")
                        for prod in produtos:
                            if prod.id == produto:
                                produto = str(ajusteProduto(prod))
                                valorTotal += int(prod.preco)
                                produtosAdicional.append(produto)
                    elif option == 'Nao':
                        id = str(random.randint(1, 1000))
                        session.execute(
                            insert, [id, usuario, str(produtosAdicional), str(valorTotal)])
                        on = False

# Updates


def updateUser():
    print()


def updateProduto():
    print()


def updateVendedor():
    print()

# Delete


def deleteUser():
    print()


def deleteProduto():
    print()


def deleteVendedor():
    print()


def deleteCompra():
    print()

# FindOneQuery


def findOneUser(alvo):
    select = session.prepare(
        "SELECT * FROM usuario WHERE email = ?")
    execute = session.execute(select, [alvo])
    for user in execute:
        print(
            f"""\nNome: {user.nome}\nEmail: {user.email}\nCPF: {user.cpf}\nEndereço: {user.end}""")


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
    execute = session.execute(select, [alvo])
    for user in execute:
        print(
            f"""\nNome: {user.nome}\nEmail: {user.email}\nCPF: {user.cpf}\nEndereço: {user.end}""")


def findOneCompra(alvo):
    select = session.prepare(
        "SELECT * FROM compra WHERE id = ?")
    execute = session.execute(select, [alvo])
    for compra in execute:
        print(
            f"""\nUsuario: {compra.usuario}\nProdutos: {compra.produto}""")


# FindManyQuery


def findManyUser():
    print()


def findManyProdutos():
    print()


def findManyVendedor():
    print()


def findManyCompra():
    print()


def main():
    findOneCompra("522")


main()


""" 
        print(f"Rua: {itens.rua}")
        print(f"Bairro: {itens.bairro}")
        print(f"Cep: {itens.cep}")
        print(f"Estado: {itens.estado}")
        print(f"Cidade: {itens.cidade}")
"""
