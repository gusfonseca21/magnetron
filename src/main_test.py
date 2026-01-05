import time

from prefect import flow, task
from prefect.futures import PrefectFuture
from prefect.task_runners import ThreadPoolTaskRunner

## ============================ TSE FLOW ========================================================


@task
def tse_lo_task_3(file: str):
    print("Iniciando Task LO 3 do flow TSE")
    time.sleep(7)
    print(f"Task LO 3 finalizou: {file}")


@task
def tse_lo_task_2(file: str):
    print("Iniciando Task LO 2 do flow TSE")
    time.sleep(5)
    print(f"Task LO 2 finalizou: {file}")


@task
def tse_lo_task_1(file: str):
    print("Iniciando Task LO 1 do flow TSE")
    time.sleep(3)
    print(f"Task LO 1 finalizou: {file}")


@task
def tse_tf_task_3(file: str):
    print("Iniciando Task TF 3 do flow TSE")
    time.sleep(15)
    return file


@task
def tse_tf_task_2(file: str):
    print("Iniciando Task TF 2 do flow TSE")
    time.sleep(4)
    return file


@task
def tse_tf_task_1(file: str):
    try:
        print("Iniciando Task TF 1 do flow TSE")
        time.sleep(10)
        return file
    except Exception as e:
        print(f"ERRO: {e}")


@task
def tse_ex_task_3():
    print("Iniciando Task EX 3 do flow TSE")
    time.sleep(10)
    return "3.json"


@task
def tse_ex_task_2():
    print("Iniciando Task EX 2 do flow TSE")
    time.sleep(6)
    return "2.json"


@task
def tse_ex_task_1():
    print("Iniciando Task EX 1 do flow TSE")
    time.sleep(5)
    return "1.json"


@flow
def tse_flow():
    print("Iniciando o flow TSE")
    ex_1 = tse_ex_task_1.submit()
    ex_2 = tse_ex_task_2.submit()
    ex_3 = tse_ex_task_3.submit()

    tf_1 = tse_tf_task_1.submit(ex_1)  # type: ignore
    tf_2 = tse_tf_task_2.submit(ex_2)  # type: ignore
    tf_3 = tse_tf_task_3.submit(ex_3)  # type: ignore

    lo_1 = tse_lo_task_1.submit(tf_1)  # type: ignore
    lo_2 = tse_lo_task_2.submit(tf_2)  # type: ignore
    lo_3 = tse_lo_task_3.submit(tf_3)  # type: ignore

    lo_1.result()
    lo_2.result()
    lo_3.result()


@task
def run_tse():
    tse_flow()


## ===============================================================================================

## ============================ CAMARA FLOW ========================================================


@task
def camara_lo_tk_3(file: str):
    print("Iniciando LO 3 Câmara")
    time.sleep(5)
    return file


@task
def camara_lo_tk_2(file: str):
    print("Iniciando LO 2 Câmara")
    time.sleep(15)
    return file


@task
def camara_lo_tk_1(file: str):
    print("Iniciando LO 1 Câmara")
    time.sleep(20)
    return file


@task
def camara_tf_tk_3(file: str):
    print("Iniciando TF 3 Câmara")
    time.sleep(22)
    return file


@task
def camara_tf_tk_2(file: str):
    print("Iniciando TF 2 Câmara")
    time.sleep(7)
    return file


@task
def camara_tf_tk_1(file: str):
    print("Iniciando TF 1 Câmara")
    time.sleep(12)
    return file


@task
def camara_ex_tk_3():
    print("Iniciando EX 3 Câmara")
    time.sleep(2)
    return "3.json"


@task
def camara_ex_tk_2():
    print("Iniciando EX 2 Câmara")
    time.sleep(16)
    return "2.json"


@task
def camara_ex_tk_1():
    print("Iniciando EX 1 Câmara")
    time.sleep(5)
    return "1.json"


@flow()
def camara_flow():
    print("Iniciando flow Câmara")
    ft_ex_camara_1 = camara_ex_tk_1.submit()
    ft_ex_camara_2 = camara_ex_tk_2.submit()
    ft_ex_camara_3 = camara_ex_tk_3.submit()

    ft_tf_camara_1 = camara_tf_tk_1.submit(ft_ex_camara_1)  # type: ignore
    ft_tf_camara_2 = camara_tf_tk_2.submit(ft_ex_camara_2)  # type: ignore
    ft_tf_camara_3 = camara_tf_tk_3.submit(ft_ex_camara_3)  # type: ignore

    ft_lo_camara_1 = camara_lo_tk_1.submit(ft_tf_camara_1)  # type: ignore
    ft_lo_camara_2 = camara_lo_tk_2.submit(ft_tf_camara_2)  # type: ignore
    ft_lo_camara_3 = camara_lo_tk_3.submit(ft_tf_camara_3)  # type: ignore

    ft_lo_camara_1.result()
    ft_lo_camara_2.result()
    ft_lo_camara_3.result()


@task
def run_camara():
    camara_flow()


## ===============================================================================================


@flow
def main_flow():
    print("Iniciando o flow principal")
    run_camara.submit()
    run_tse.submit()


if __name__ == "__main__":
    main_flow.serve(name="deployment")
