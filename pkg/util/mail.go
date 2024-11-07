package util

import (
	"fmt"
	"mime"

	"github.com/rs/zerolog/log"
	"gopkg.in/gomail.v2"
)

type SMTPConfig struct {
	Server MaildServer `yaml:"server"`
	To     []string    `yaml:"to"`
}

type MaildServer struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"` //defaut 465
	UserAddress string `yaml:"user_address"`
	UserName    string `yaml:"user_name"`
	Password    string `yaml:"password"`
}

func MailTo(mailServer MaildServer, subject, body string, to []string, attFiles map[string]string) error {
	if mailServer.Port == 0 {
		mailServer.Port = 465
	}
	m := gomail.NewMessage(gomail.SetEncoding(gomail.Base64))
	m.SetHeader("From", m.FormatAddress(mailServer.UserAddress, mailServer.UserName))
	m.SetHeader("To", to...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", string(body))

	if nil != attFiles {
		for name, filepath := range attFiles {
			attCtxDisposition := []string{fmt.Sprintf(`attachment; filename="%s"`, mime.QEncoding.Encode("UTF-8", name))}
			attHeader := map[string][]string{"Content-Disposition": attCtxDisposition}
			m.Attach(filepath, gomail.Rename(name), gomail.SetHeader(attHeader))
		}
	}

	d := gomail.NewDialer(mailServer.Host, 465, mailServer.UserAddress, mailServer.Password)
	d.SSL = true
	err := d.DialAndSend(m)
	logger := log.Info().Any("Tag", "Mail").Any("subject", subject).Any("body", body).Err(err)
	logger.Msg("send mail")

	return err
}
