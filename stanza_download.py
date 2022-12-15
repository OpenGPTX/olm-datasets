import stanza
import os

working_dir=os.getenv('WORKING_DIR')

for lang in {"ar", "ca", "eu", "id", "vi", "zh-hans", "zh-hant","de","en"}:
    stanza.download(lang,model_dir=f'{working_dir}_stanza_models')
