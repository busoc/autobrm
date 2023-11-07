#!/usr/bin/awk -f

function trimSpace(str) {
  # tmp = str
  # gsub(/ /, "", tmp)
  return str
}

function checkGap(hrd, vmu) {
  delta = $10 - hrd["seq"]
  if (delta > 1 && delta != $10) {
    vmudiff = ($4 - vmu["seq"])-1
    chan = trimSpace($7)
    vtime = trimSpace(vmu["time"])
    htime = trimSpace(hrd["time"])
    # upi = cutString(trimSpace(hrd["upi"]))
    upi = hrd["upi"]
    if (index(upi, "*") > 0) {
      upi = "?"
    }
    printf(gaprow, chan, vtime, trimSpace($3), vmu["seq"], $4, vmudiff, trimSpace($8), htime, $9, hrd["seq"], $10, delta-1, upi)
  } else {
    delta = 1
  }
  return delta - 1
}

function checkBad(bad, chan, origin, vmu) {
  bad = trimSpace(bad)
  if (bad == "invalid" || bad == "bad") {
    return
  }
  chan = trimSpace(chan)
  origin = trimSpace(origin)

  if (chan in baddata && origin in baddata[chan]) {
    # delta = vmu["seq"] - baddata[chan][origin]["seq"] - 1
    # if (delta < 0) {
    #   delta = 0
    # }
    #
    # time = baddata[chan][origin]["time"]
    # if (time == "") {
    #   time = vmu["time"]
    # }
    dtstart = baddata[chan][origin]["dtstart"]
    dtend = baddata[chan][origin]["dtend"]
    first = baddata[chan][origin]["first"]
    last = baddata[chan][origin]["last"]

    delta = last - first
    if (delta <= 0) {
      last = first
      dtend = dtstart
      delta = 1
    }
    printf(badrow, chan, dtstart, dtend, first, last, delta, origin)
    # printf(badrow, chan, time, vmu["time"], baddata[chan][origin]["seq"], vmu["seq"], delta, origin)

    delete baddata[chan][origin]
  }
}

function keepOrigin(origin) {
  switch (origin) {
  default:
    return 0
  case /3[3-9]|4[0-7]|51|90/:
    return 1
  }
}

BEGIN{
  # FS=/[[:space:]]+\\|[[:space:]]+/
  FS="([[:space:]]+\\|+[[:space:]]+)|,|;"
  #channel, vmu dtstart, vmu dtend, vmu first, vmu last, vmu delta, hrd origin, hrd dtstart, hrd dtend, hrd start, hrd end
  # gaprow = "G | %4s | %s | %8d | %8d | %4d || %2d | %s | %s | %8d | %8d | %4d | %s\n"
  gaprow = "G | %4s | %s | %s | %8d | %8d | %8d || %2d | %s | %s | %8d | %8d | %4d | %s\n"
  badrow = "B | %4s | %s | %s | %8d | %8d | %8d || %2d\n"
}
NF < 11{
  next
}
/\s+(invalid|bad)\s+|invalid/ {
# /$14=="invalid" || $14=="bad"/ {
  chan = trimSpace($7)
  switch (chan) {
  case "lrsd":
    lrsd["bad"]++
    break
  case "vic1":
    vic1["bad"]++
    break
  case "vic2":
    vic2["bad"]++
    break
  }
  if (keepOrigin($8) == 0) {
    next
  }
  origin = trimSpace($8)
  baddata[chan][origin]["count"]++
  if (!("seq" in baddata[chan][origin])) {
    baddata[chan][origin]["seq"] = $4
    baddata[chan][origin]["time"] = $3
    baddata[chan][origin]["first"] = $4
    baddata[chan][origin]["dtstart"] = $3
  } else {
    baddata[chan][origin]["last"] = $4
    baddata[chan][origin]["dtend"] = $3
  }
  # next
}
/lrsd/{
  lrsd["total"]++
  if (keepOrigin($8) == 0) {
    next
  }
  origin = trimSpace($8)
  if (origin in sciences) {
    lrsd["missing"] += checkGap(sciences[origin], lrsd)
  }
  lrsd["time"] = $3
  lrsd["seq"] = $4
  checkBad($14, $7, $8, lrsd)
  sciences[origin]["time"] = $9
  sciences[origin]["seq"] = $10
  sciences[origin]["upi"] = $11
}
/vic1/{
  vic1["total"]++
  if (keepOrigin($8) == 0) {
    next
  }
  origin = trimSpace($8)
  if (origin in vicone) {
    vic1["missing"] += checkGap(vicone[origin], vic1)
  }
  vic1["time"] = $3
  vic1["seq"] = $4
  checkBad($14, $7, $8, vic1)
  vicone[origin]["time"] = $9
  vicone[origin]["seq"] = $10
  vicone[origin]["upi"] = $11
}
/vic2/{
  vic2["total"]++
  if (keepOrigin($8) == 0) {
    next
  }
  origin = trimSpace($8)
  if (origin in victwo) {
    vic2["missing"] += checkGap(victwo[origin], vic2)
  }
  vic2["time"] = $3
  vic2["seq"] = $4
  checkBad($14, $7, $8, vic2)
  victwo[origin]["time"] = $9
  victwo[origin]["seq"] = $10
  victwo[origin]["upi"] = $11
}
END{
  for (chan in baddata) {
    switch (chan) {
    default:
      continue
    case "lrsd":
      for (k in lrsd) vmu[k] = lrsd[k]
      break
    case "vic1":
      for (k in vic1) vmu[k] = vic1[k]
      break
    case "vic2":
      for (k in vic2) vmu[k] = vic2[k]
      break
    }
    for (origin in baddata[chan]) {
      checkBad("", chan, origin, vmu)
      for (origin in baddata[chan]) badvic2 += baddata[chan][origin]["count"]
    }
  }
  # print
  printf("\n%d VMU packets\n", lrsd["total"]+vic1["total"]+vic2["total"])
  printf("missing %d LRSD packets (total: %d, bad: %d) \n", lrsd["missing"], lrsd["total"], lrsd["bad"])
  printf("missing %d VIC1 packets (total: %d, bad: %d)\n", vic1["missing"], vic1["total"], vic1["bad"])
  printf("missing %d VIC2 packets (total: %d, bad: %d)\n", vic2["missing"], vic2["total"], vic2["bad"])
  print "\n"
}
