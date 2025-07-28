package main

import (
	goflag "flag"
	"fmt"
	"os"

	// "github.com/natamm4/audit-tool/pkg/cmd/gather"
	"github.com/natamm4/audit-tool/pkg/cmd/get"
	"github.com/natamm4/audit-tool/pkg/cmd/query"
	"github.com/natamm4/audit-tool/pkg/cmd/visualize"

	"k8s.io/cli-runtime/pkg/genericclioptions"

	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	utilflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
)

func main() {

	pflag.CommandLine.SetNormalizeFunc(utilflag.WordSepNormalizeFunc)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	logs.InitLogs()
	defer logs.FlushLogs()

	logrus.SetOutput(logs.KlogWriter{})
	logrus.SetFormatter(&logrus.TextFormatter{})

	command := NewAuditToolCommand()
	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)

	}
}
func NewAuditToolCommand() *cobra.Command {
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

	cmd.AddCommand(get.NewCommand(f, ioStreams))
	cmd.AddCommand(query.NewCommand(f, ioStreams))
	cmd.AddCommand(visualize.NewCommand(f, ioStreams))

	return cmd
}
