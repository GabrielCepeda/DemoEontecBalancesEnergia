# Bienvenidos a la Demo EONTEC de Balances de Energía

## Introducción

En este repositorio encuentran el notebook y el archivo que se utiliza para demostrar como se calculan los balances de energía en una plataforma como Databricks.

A continuación se encuentran las instrucciones para que puedan utilizarlo o replicarlo después por su cuenta:

## Instrucciones

### 1. Crea tu cuenta en Databricks

Si no lo has hecho, crea tu cuenta gratuita en  [Databricks Community Edition](https://community.cloud.databricks.com).

### 2. Instala git en tu computadora:

Sigue los pasos acorde a tu máquina.

#### Para Sistemas Linux (Ubuntu/Debian, Fedora/CentOS, etc.)

##### 2.1. Instalación de Git con apt (Ubuntu/Debian):

```bash
sudo apt-get install git
```
##### 2.2. Instalación de Git con dnf (Fedora/CentOS):

```bash
sudo dnf install git
```
#### Para macOS

##### 2.3. Instalación de Git con Homebrew:

```bash
# Instala Homebrew si aún no lo tienes:
/bin/bash -c "$(curl -fsSL [https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh](https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh))"   

# Instala Git con Homebrew:
brew install git
```

#### Para Windows

##### 2.4. Instalación de Git con PowerShell:
Descarga el instalador desde [aquí](https://git-scm.com/download/win) y sigue las instrucciones.

### 3. Instalación de SSH (Opcional):

#### Linux/macOS: 

Generalmente viene preinstalado. Si no, utiliza el gestor de paquetes de tu distribución.

#### Windows 

SSH no viene instalado por defecto. Puedes habilitarlo como una característica opcional:

```Programas_y_Caracteristicas
Abre Settings > Apps > Apps & Features > Optional Features.

Busca "OpenSSH Client" y/or "OpenSSH Server" y instálalos si es necesario.
```

### 4. Crea una cuenta gratuita en [github](https://github.com/join).

### 5. Generación de una Clave SSH:

#### para Linux y Mac

```bash
ssh-keygen -t rsa -b 4096
```
#### Para Windows con Power Shell

```powershell
ssh-keygen -t rsa -b 4096
```

### 6. Subida de la Clave SSH a GitHub:

En GitHub, ve a Settings > SSH and GPG keys
Agrega la clave pública (el contenido del archivo .pub generado en el paso anterior).

### 7. Clonación del Repositorio:

#### para Linux y Mac

```bash
cd tu/ruta/deseada
git clone git@github.com:GabrielCepeda/DemoEontecBalancesEnergia.git
```

#### para Windows (PowerShell):

```powershell
cd C:\ruta\a\tu\carpeta
git clone git@github.com:GabrielCepeda/DemoEontecBalancesEnergia.git
```

### 8. Utiliza el siguiente comando en la consola para descargar el repositorio completo

```console
git pull
```

### 9. Carga del Notebook con Databricks
Busca la opción **workspace**, en el panel que se abre, **haz click derecho con tu mouse** y selecciona la opción **import**, busca, o selecciona y arrastra el notebook que acabas de descargarde este repo y **cárgalo en databricks**.   