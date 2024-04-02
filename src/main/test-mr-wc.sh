#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}


TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# Start timer
start=$(date +%s.%N)

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 1
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 2
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 3
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 4
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 5
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 6
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 7
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 8
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 9
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 10

# wait for the coordinator to exit.
wait $pid

# Stop timer
end=$(date +%s.%N)

# Calculate time taken
runtime=$(python3 -c "print(${end} - ${start})")

echo '---' wc test took $runtime seconds.

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

# wait for user to press enter
echo 'Press Enter to continue'
read
