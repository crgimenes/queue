package queue

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nuveo/beanstalk"
)

func TestConnectLoop(t *testing.T) {
	type args struct {
		tube    string
		retries int
	}
	tests := []struct {
		name     string
		args     args
		wantConn *beanstalk.Conn
		wantErr  bool
		setup    func()
		teardown func()
	}{
		{
			name:    "success",
			wantErr: false,
			args: args{
				retries: 0,
			},
			setup: func() {
				bs.Conn = func(tube string) (c *beanstalk.Conn, err error) {
					return
				}
			},
			teardown: func() {
				bs.Conn = Conn
			},
		},
		{
			name:    "error",
			wantErr: true,
			args: args{
				retries: 0,
			},
			setup: func() {
				bs.Conn = func(tube string) (c *beanstalk.Conn, err error) {
					err = errors.New("test error")
					return
				}
			},
			teardown: func() {
				bs.Conn = Conn
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			defer func() {
				if tt.teardown != nil {
					tt.teardown()
				}
			}()
			gotConn, err := ConnectLoop(tt.args.tube, tt.args.retries)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectLoop() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotConn, tt.wantConn) {
				t.Errorf("ConnectLoop() = %v, want %v", gotConn, tt.wantConn)
			}

		})
	}
}
