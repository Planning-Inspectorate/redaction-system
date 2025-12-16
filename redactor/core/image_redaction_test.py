from redactor.core.redaction.redactor.image_redactor import ImageRedactor
from redactor.core.redaction.config.redaction_config.image_redaction_config import ImageRedactionConfig
from PIL import Image
from dotenv import load_dotenv


load_dotenv(verbose=True, override=True)


img = Image.open("samples/rots.jpeg")


redactor = ImageRedactor(
    ImageRedactionConfig(
        name="demo",
        redactor_type="ImageRedaction",
        image=img
    )
)

redacted = redactor.redact()