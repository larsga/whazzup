function sendvote(action, id) {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "/vote/" + action + "/" + id, false);
  xmlhttp.send("");

  var elem = document.getElementById("error");

  var status = (xmlhttp.readyState == 4 && xmlhttp.status == 200);
  if (!status) {
    if (xmlhttp.status == 0)
      msg = "No response from server";
    else
      msg = "" + xmlhttp.status + ": "  + xmlhttp.statusText + " (" +
            xmlhttp.readyState + ")"

    elem.childNodes[0].nodeValue = msg;
    elem.className = "errorshow";
  } else {
    // if the error was visible already, but this request went through
    // we need to blank out the error display
    elem.className = "error";
  }

  return status;
}

function vote(action, index, id) {
  if (!sendvote(action, id)) {
    return; // problem already reported by sendvote
  }

  var elem = document.getElementById("storytable");
  var rows = elem.getElementsByTagName("tr");

  rows[index * 2].className = "hidden";
  rows[index * 2 + 1].className = "hidden";

  for (ix = Math.max(24, index + 1); ix < 30; ix++) {
    if (rows[ix * 2].className == "hidden") {
      rows[ix * 2].className = "visible";
      rows[ix * 2+1].className = "visible";

      var newid = rows[ix * 2].id;
      var marlink = document.getElementById("mark-as-read-link");
      marlink.href += "," + newid;
      break;
    }
  }

  if (ix == 29) {
    window.location.reload();                      
  }
}
