package utils

import (
	"fmt"
	"net"
	"regexp"

	"github.com/Odinman/ogo/utils"
	zmq "github.com/pebbe/zmq4"
)

var (
	all_char = regexp.MustCompile("^[^[:cntrl:]]*$")
)

func GetLocalIdentity(namespace string) (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var ha net.HardwareAddr
	for _, inter := range interfaces {
		//fmt.Println(inter.Name, inter.HardwareAddr)
		if len(inter.HardwareAddr) > 0 {
			//fmt.Println(inter.HardwareAddr)
			ha = inter.HardwareAddr
			//获取第一个
			break
		}
	}

	identity := utils.NewShortUUID5(namespace, ha.String())

	return identity, nil
}

func Dump(soc *zmq.Socket) {
	fmt.Println("----------------------------------------")
	for {
		//  Process all parts of the message
		message, _ := soc.Recv(0)

		//  Dump the message as text or binary
		fmt.Printf("[%03d] ", len(message))
		if all_char.MatchString(message) {
			fmt.Print(message)
		} else {
			for i := 0; i < len(message); i++ {
				fmt.Printf("%02X ", message[i])
			}
		}
		fmt.Println()

		more, _ := soc.GetRcvmore()
		if !more {
			break
		}
	}
}

/* {{{ func Unwrap(msg []string) (head string, tail []string)
 *  Pops frame off front of message and returns it as 'head'
 *  If next frame is empty, pops that empty frame.
 *  Return remaining frames of message as 'tail'
 */
func Unwrap(msg []string) (head string, tail []string) {
	head = msg[0]
	if len(msg) > 1 && msg[1] == "" {
		tail = msg[2:]
	} else {
		tail = msg[1:]
	}
	return
}

/* }}} */
