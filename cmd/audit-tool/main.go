package main

import (
	"context"
	goflag "flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/mfojtik/audit-tool/pkg/cmd/query"

	"k8s.io/cli-runtime/pkg/genericclioptions"

	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/mfojtik/audit-tool/pkg/cmd/get"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	utilflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	pflag.CommandLine.SetNormalizeFunc(utilflag.WordSepNormalizeFunc)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	logs.InitLogs()
	defer logs.FlushLogs()

	logrus.SetOutput(logs.KlogWriter{})
	logrus.SetFormatter(&logrus.TextFormatter{})

	ctx := context.TODO()

	command := NewAuditToolCommand(ctx)
	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)

	}
}
func NewAuditToolCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit-tool",
		Short: "Allows to operate on Kubernetes API server audit logs",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
			os.Exit(1)
		},
	}

	kubeConfigFlags := genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag()
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)
	matchVersionKubeConfigFlags.AddFlags(cmd.PersistentFlags())

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)
	ioStreams := genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}

	cmd.AddCommand(get.NewCommand(ctx, f, ioStreams))
	cmd.AddCommand(query.NewCommand(ctx, f, ioStreams))

	return cmd
}
