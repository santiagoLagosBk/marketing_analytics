# Necessary Run
 Go to current folder.
 * `PS> python -m venv venv`
 * `PS> .\venv\Scripts\Activate.ps1`
 * `PS> pip3 install -r requirements.txt`

 ### Windows warning: If there is an error.
` .\venv\Scripts\Activate.ps1 : No se puede cargar el archivo` `.\venv\Scripts\Activate.ps1 porque la ejecución de scripts está deshabilitada en este sistema. Para obtener más información, consulta el tema about_Execution_Policies en https:/go.microsoft.com/fwlink/?LinkID=135170. En línea: 1 Carácter: 1`

 ### Execute on the PowerShell:
 * `PS> Set-ExecutionPolicy RemoteSigned`

To manually create a virtualenv on MacOS and Linux:

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt