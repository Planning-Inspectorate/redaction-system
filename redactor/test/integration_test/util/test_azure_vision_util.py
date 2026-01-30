import os

from PIL import Image
from io import BytesIO

from core.util.azure_vision_util import AzureVisionUtil


def test__azure_vision_util__detect_faces():
    """
    - Given I have an image with two people in it (Darth Plagueis the wise scene from Revenge of the Sith)
    - When I call AzureVisionUtil.detect_faces
    - The two faces should be identified
    """
    with open(
        os.path.join("test", "resources", "image", "image_with_faces.jpeg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_faces(image, confidence_threshold=0.5)
        # Azure Vision seems to be deterministic from testing

    expected_response = ((0, 4, 409, 427), (360, 7, 407, 424))
    assert expected_response == response


def test__azure_vision_util__detect_faces__use_cached_result():
    with open(
        os.path.join("test", "resources", "image", "image_with_faces.jpeg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_faces(image, confidence_threshold=0.5)
        # Azure Vision seems to be deterministic from testing
        new_response = AzureVisionUtil().detect_faces(image, confidence_threshold=0.5)

    expected_response = ((0, 4, 409, 427), (360, 7, 407, 424))

    assert expected_response == new_response
    assert response == new_response


# Azure Vision seems to be deterministic from testing
EXPECTED_TEXT_RESPONSE = (
    ("You", (11, 13, 74, 57)),
    ("see,", (87, 13, 161, 57)),
    ("he's", (170, 13, 240, 58)),
    ("met", (250, 14, 317, 58)),
    ("two", (327, 14, 388, 58)),
    ("of", (401, 14, 431, 58)),
    ("your", (441, 14, 520, 58)),
    ("three", (529, 14, 619, 58)),
    ("criteria", (629, 14, 747, 58)),
    ("for", (757, 14, 805, 58)),
    ("sentience,", (814, 14, 993, 57)),
    ("so", (1002, 14, 1041, 57)),
    ("what", (1051, 14, 1135, 57)),
    ("if", (1145, 14, 1167, 57)),
    ("he", (1176, 14, 1220, 57)),
    ("meets", (1230, 14, 1338, 56)),
    ("the", (1347, 14, 1402, 56)),
    ("third.", (1413, 14, 1503, 55)),
    ("Consciousness", (10, 66, 274, 108)),
    ("in", (283, 66, 313, 108)),
    ("even", (326, 66, 410, 109)),
    ("the", (419, 65, 475, 109)),
    ("smallest", (488, 65, 629, 109)),
    ("degree.", (638, 65, 771, 109)),
    ("What", (780, 65, 873, 109)),
    ("is", (883, 65, 913, 109)),
    ("he", (923, 65, 966, 109)),
    ("then?", (977, 65, 1075, 109)),
    ("I", (1085, 65, 1097, 109)),
    ("don't", (1107, 65, 1191, 109)),
    ("know.", (1201, 65, 1302, 109)),
    ("Do", (1312, 66, 1362, 109)),
    ("you?", (1372, 66, 1458, 109)),
    ("(to", (1468, 66, 1514, 109)),
    ("Riker)", (12, 116, 114, 161)),
    ("Do", (124, 116, 172, 161)),
    ("you?", (182, 116, 269, 162)),
    ("(to", (279, 116, 325, 162)),
    ("Phillipa)", (335, 116, 477, 162)),
    ("Do", (487, 116, 536, 162)),
    ("you?", (546, 116, 630, 162)),
    ("Well,", (640, 116, 727, 162)),
    ("that's", (737, 116, 832, 162)),
    ("the", (842, 116, 896, 162)),
    ("question", (907, 116, 1054, 161)),
    ("you", (1065, 116, 1128, 161)),
    ("have", (1139, 117, 1222, 161)),
    ("to", (1233, 117, 1268, 160)),
    ("answer.", (1280, 117, 1416, 159)),
    ("Your", (1426, 118, 1507, 159)),
    ("Honour,", (10, 170, 147, 211)),
    ("the", (156, 169, 210, 211)),
    ("courtroom", (222, 169, 386, 212)),
    ("is", (407, 169, 439, 212)),
    ("a", (449, 169, 469, 213)),
    ("crucible.", (479, 168, 625, 213)),
    ("In", (634, 168, 668, 213)),
    ("it", (678, 168, 699, 213)),
    ("we", (708, 168, 759, 213)),
    ("burn", (769, 168, 847, 213)),
    ("away", (860, 168, 949, 213)),
    ("irrelevancies", (959, 169, 1179, 212)),
    ("until", (1188, 169, 1262, 211)),
    ("we", (1271, 170, 1322, 211)),
    ("are", (1335, 170, 1389, 210)),
    ("left", (1398, 170, 1452, 210)),
    ("with", (1462, 170, 1532, 209)),
    ("a", (1544, 171, 1565, 209)),
    ("pure", (13, 222, 86, 264)),
    ("product,", (99, 221, 239, 264)),
    ("the", (249, 221, 303, 264)),
    ("truth", (315, 221, 391, 264)),
    ("for", (404, 220, 453, 264)),
    ("all", (462, 220, 500, 263)),
    ("time.", (510, 220, 595, 263)),
    ("Now,", (604, 220, 695, 263)),
    ("sooner", (704, 220, 822, 263)),
    ("or", (832, 220, 867, 263)),
    ("later,", (876, 220, 964, 263)),
    ("this", (973, 220, 1033, 262)),
    ("man", (1043, 220, 1119, 262)),
    ("or", (1132, 220, 1167, 262)),
    ("others", (1176, 220, 1283, 262)),
    ("like", (1293, 220, 1353, 262)),
    ("him", (1362, 221, 1416, 261)),
    ("will", (1437, 221, 1494, 261)),
    ("succeed", (12, 274, 154, 316)),
    ("in", (164, 273, 194, 316)),
    ("replicating", (205, 272, 384, 316)),
    ("Commander", (397, 272, 611, 316)),
    ("Data.", (621, 271, 717, 316)),
    ("And", (727, 271, 794, 316)),
    ("the", (805, 271, 860, 316)),
    ("decision", (871, 271, 1012, 316)),
    ("you", (1023, 271, 1087, 316)),
    ("reach", (1098, 272, 1193, 316)),
    ("here", (1204, 272, 1283, 315)),
    ("today", (1293, 272, 1389, 315)),
    ("will", (1399, 273, 1458, 315)),
    ("determine", (11, 325, 183, 367)),
    ("how", (193, 324, 257, 367)),
    ("we", (273, 324, 323, 368)),
    ("will", (333, 324, 392, 368)),
    ("regard", (402, 324, 514, 368)),
    ("this", (524, 323, 586, 368)),
    ("creation", (595, 323, 733, 368)),
    ("of", (747, 323, 779, 368)),
    ("our", (789, 323, 848, 368)),
    ("genius.", (858, 323, 982, 368)),
    ("It", (992, 323, 1016, 368)),
    ("will", (1026, 323, 1082, 368)),
    ("reveal", (1092, 323, 1196, 368)),
    ("the", (1206, 324, 1261, 368)),
    ("kind", (1271, 324, 1343, 367)),
    ("of", (1357, 324, 1389, 367)),
    ("a", (1400, 324, 1421, 367)),
    ("people", (1431, 324, 1546, 367)),
    ("we", (1556, 325, 1608, 366)),
    ("are,", (12, 377, 77, 417)),
    ("what", (86, 376, 170, 418)),
    ("he", (179, 376, 223, 418)),
    ("is", (233, 375, 264, 418)),
    ("destined", (272, 375, 422, 419)),
    ("to", (431, 374, 464, 419)),
    ("be.", (474, 374, 530, 419)),
    ("It", (539, 374, 561, 419)),
    ("will", (570, 374, 626, 419)),
    ("reach", (635, 374, 733, 419)),
    ("far", (745, 374, 793, 419)),
    ("beyond", (802, 374, 931, 419)),
    ("this", (941, 374, 1003, 419)),
    ("courtroom", (1012, 374, 1177, 418)),
    ("and", (1201, 374, 1265, 418)),
    ("this", (1275, 374, 1337, 418)),
    ("one", (1346, 375, 1412, 417)),
    ("android.", (1425, 375, 1563, 416)),
    ("It", (1572, 376, 1600, 416)),
    ("could", (11, 429, 103, 471)),
    ("significantly", (116, 428, 315, 471)),
    ("redefine", (324, 427, 466, 471)),
    ("the", (475, 427, 531, 471)),
    ("boundaries", (540, 427, 737, 471)),
    ("of", (746, 426, 779, 471)),
    ("personal", (788, 426, 939, 471)),
    ("liberty", (948, 427, 1055, 471)),
    ("and", (1067, 427, 1129, 471)),
    ("freedom,", (1139, 427, 1295, 471)),
    ("expanding", (1304, 427, 1485, 470)),
    ("them", (1495, 428, 1571, 470)),
    ("for", (8, 480, 60, 521)),
    ("some,", (68, 480, 176, 522)),
    ("savagely", (184, 479, 338, 522)),
    ("curtailing", (346, 478, 504, 523)),
    ("them", (514, 478, 589, 523)),
    ("for", (610, 478, 658, 523)),
    ("others.", (667, 478, 792, 523)),
    ("Are", (801, 477, 859, 523)),
    ("you", (869, 477, 933, 523)),
    ("prepared", (943, 477, 1100, 522)),
    ("to", (1110, 477, 1143, 522)),
    ("condemn", (1153, 477, 1313, 521)),
    ("him", (1323, 478, 1375, 520)),
    ("and", (1400, 478, 1463, 520)),
    ("all", (1476, 478, 1512, 519)),
    ("who", (1522, 478, 1597, 518)),
    ("come", (10, 531, 106, 573)),
    ("after", (118, 531, 195, 573)),
    ("him", (203, 530, 257, 573)),
    ("to", (277, 530, 311, 574)),
    ("servitude", (323, 530, 481, 574)),
    ("and", (490, 529, 555, 574)),
    ("slavery?", (567, 529, 715, 574)),
    ("Your", (724, 529, 803, 574)),
    ("Honour,", (812, 529, 954, 574)),
    ("Starfleet", (963, 529, 1104, 574)),
    ("was", (1113, 529, 1187, 573)),
    ("founded", (1196, 529, 1337, 572)),
    ("to", (1347, 530, 1382, 572)),
    ("seek", (1392, 530, 1473, 571)),
    ("out", (1483, 530, 1542, 571)),
    ("new", (9, 581, 74, 624)),
    ("life.", (89, 581, 152, 624)),
    ("Well,", (160, 581, 249, 625)),
    ("there", (258, 581, 349, 625)),
    ("it", (358, 581, 384, 625)),
    ("sits.", (392, 581, 461, 626)),
    ("Waiting.", (469, 580, 613, 626)),
    ("You", (621, 580, 684, 626)),
    ("wanted", (692, 580, 824, 626)),
    ("a", (835, 580, 856, 626)),
    ("chance", (864, 580, 993, 625)),
    ("to", (1001, 581, 1035, 625)),
    ("make", (1044, 581, 1141, 625)),
    ("law.", (1150, 581, 1219, 624)),
    ("Well,", (1227, 581, 1317, 624)),
    ("here", (1324, 581, 1405, 623)),
    ("it", (1413, 581, 1437, 623)),
    ("is.", (1445, 581, 1485, 623)),
    ("Make", (1493, 581, 1591, 622)),
    ("a", (12, 638, 33, 676)),
    ("good", (44, 637, 128, 676)),
    ("one.", (140, 635, 219, 675)),
)


def test__azure_vision_util__detect_text():
    """
    - Given I have an image containing a lot of text
    - When I call AzureVisionUtil.detect_text
    - The text content of the image should be extracted, with each line represented by a bounding box
    """
    with open(
        os.path.join("test", "resources", "image", "image_with_text.jpg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_text(image)

    assert EXPECTED_TEXT_RESPONSE == response


def test__azure_vision_util__detect_text__use_cached_result():
    with open(
        os.path.join("test", "resources", "image", "image_with_text.jpg"),
        "rb",
    ) as f:
        image = Image.open(BytesIO(f.read()))
        response = AzureVisionUtil().detect_text(image)
        new_response = AzureVisionUtil().detect_text(image)

    assert EXPECTED_TEXT_RESPONSE == response
    assert response == new_response
