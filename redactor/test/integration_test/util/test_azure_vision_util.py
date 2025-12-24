from redactor.core.util.ai.azure_vision_util import AzureVisionUtil
from PIL import Image
from io import BytesIO


def test__azure_vision_util__detect_faces():
    """
    - Given I have an image with two people in it (Darth Plagueis the wise scene from Revenge of the Sith)
    - When I call AzureVisionUtil.detect_faces
    - The two faces should be identified
    """
    with open("redactor/test/resources/image_with_faces.jpeg", "rb") as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_faces(image, confidence_threshold=0.5)
        # Azure Vision seems to be deterministic from testing
        expected_response = ((0, 4, 409, 427), (360, 7, 407, 424))
        assert expected_response == response


def test__azure_vision_util__detect_text():
    """
    - Given I have an image containing a lot of text
    - When I call AzureVisionUtil.detect_text
    - The text content of the image should be extracted, with each line represented by a bounding box
    """
    with open("redactor/test/resources/image_with_text.jpg", "rb") as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_text(image)
        # Azure Vision seems to be deterministic from testing
        expected_response = (
            (
                "You see, he's met two of your three criteria for sentience, so what if he meets the third.",
                (9, 13, 1503, 13),
            ),
            (
                "Consciousness in even the smallest degree. What is he then? I don't know. Do you? (to",
                (9, 65, 1517, 65),
            ),
            (
                "Riker) Do you? (to Phillipa) Do you? Well, that's the question you have to answer. Your",
                (11, 115, 1508, 115),
            ),
            (
                "Honour, the courtroom is a crucible. In it we burn away irrelevancies until we are left with a",
                (9, 168, 1569, 168),
            ),
            (
                "pure product, the truth for all time. Now, sooner or later, this man or others like him will",
                (11, 220, 1496, 219),
            ),
            (
                "succeed in replicating Commander Data. And the decision you reach here today will",
                (8, 271, 1458, 270),
            ),
            (
                "determine how we will regard this creation of our genius. It will reveal the kind of a people we",
                (9, 323, 1613, 323),
            ),
            (
                "are, what he is destined to be. It will reach far beyond this courtroom and this one android. It",
                (9, 374, 1601, 373),
            ),
            (
                "could significantly redefine the boundaries of personal liberty and freedom, expanding them",
                (9, 426, 1586, 426),
            ),
            (
                "for some, savagely curtailing them for others. Are you prepared to condemn him and all who",
                (8, 478, 1601, 477),
            ),
            (
                "come after him to servitude and slavery? Your Honour, Starfleet was founded to seek out",
                (9, 529, 1542, 528),
            ),
            (
                "new life. Well, there it sits. Waiting. You wanted a chance to make law. Well, here it is. Make",
                (8, 580, 1595, 580),
            ),
            ("a good one.", (9, 636, 218, 633)),
        )
        assert expected_response == response
