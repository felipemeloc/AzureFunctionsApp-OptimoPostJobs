import os

env_vars = {
# Project folder
'MAIN_PATH' : 'OptimoPostJobs',

# Optimo Api Key
'AUTH_KEY' : '1d6beedd4350ea9c993449650a8aec73Aema2hVqY6U',

# Database
'SERVER' : 'tcp:soterlive1.database.windows.net.',
'DATABASE' : 'Soter_live',
'USER_NAME' : 'tylerzipfell',
'DATABASE_PASSWORD' : 'S0t3rTyl3r',

# Dev Database
'SERVER_DEV' : 'soterdev.database.windows.net',
'DATABASE_DEV' : 'Soter_dev',
'USER_NAME_DEV' : 'Soter_Wiki',
'PASSWORD_DEV' :'nLxgA6*7_D6s',

}

def load_env():
    for key, val in env_vars.items():
        os.environ[key] = val

load_env()