package get

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/kubectl/pkg/scheme"

	"k8s.io/klog/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/kubectl/pkg/util/term"

	"k8s.io/kubectl/pkg/util/interrupt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

type Options struct {
	Config *restclient.Config
	client kubernetes.Interface

	targetDirectory string

	Executor *DefaultRemoteExecutor
	StreamOptions

	genericclioptions.IOStreams
}
type StreamOptions struct {
	genericclioptions.IOStreams
	InterruptParent *interrupt.Handler
	Stdin           bool
	TTY             bool
	overrideStreams func() (io.ReadCloser, io.Writer, io.Writer)
	isTerminalIn    func(t term.TTY) bool
	Quiet           bool
}

// DefaultRemoteExecutor is the standard implementation of remote command execution
type DefaultRemoteExecutor struct{}

func (*DefaultRemoteExecutor) Execute(method string, url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error {
	exec, err := remotecommand.NewSPDYExecutor(config, method, url)
	if err != nil {
		return err
	}
	return exec.Stream(remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            stderr,
		Tty:               tty,
		TerminalSizeQueue: terminalSizeQueue,
	})
}

func NewCommand(ctx context.Context, f cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := &Options{
		StreamOptions: StreamOptions{
			IOStreams: streams,
		},

		Executor: &DefaultRemoteExecutor{},
	}
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get the audit logs from the remote masters",
		Run: func(cmd *cobra.Command, args []string) {
			argsLenAtDash := cmd.ArgsLenAtDash()
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Complete(f, cmd, args, argsLenAtDash))
			cmdutil.CheckErr(options.Run(ctx))
		},
	}

	cmd.Flags().StringVarP(&options.targetDirectory, "output", "o", "", "Output directory to store the log")

	return cmd
}

func (o *Options) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {
	var err error
	o.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}
	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	o.client = clientset

	if err := os.MkdirAll(o.targetDirectory, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (o *Options) findAPIServerPods(ctx context.Context) ([]string, error) {
	pods, err := o.client.CoreV1().Pods("openshift-kube-apiserver").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, p := range pods.Items {
		// skip installer and pruner pods
		if !strings.HasPrefix(p.Name, "kube-apiserver-") {
			continue
		}
		for _, c := range p.Status.ContainerStatuses {
			if c.Name != "kube-apiserver" {
				continue
			}
			if c.State.Running != nil && c.Ready == true {
				result = append(result, p.Name)
			}
		}
	}
	return result, nil
}

func (o *Options) getAPIServerLogs(apiserverName string) ([]string, error) {
	restClient, err := restclient.RESTClientFor(o.Config)
	if err != nil {
		return nil, err
	}
	t := o.SetupTTY()
	sizeQueue := t.MonitorSize(t.GetSize())

	files := []string{}

	// first copy the rotated audit logs as they are safe to copy
	rotatedRequest := restClient.Post().
		Resource("pods").
		Name(apiserverName).
		Namespace("openshift-kube-apiserver").
		SubResource("exec")
	rotatedRequest.VersionedParams(&corev1.PodExecOptions{
		Container: "kube-apiserver",
		TTY:       t.Raw,
		Stdout:    true,
		Command:   []string{"/bin/bash", "-c", "cd /var/log/kube-apiserver && tar -czO audit-*"},
	}, scheme.ParameterCodec)

	apiServerTargetDirectory := filepath.Join(o.targetDirectory, apiserverName)

	rotatedAuditFile, err := ioutil.TempFile(apiServerTargetDirectory, "rotated-audit-logs")
	if err != nil {
		return nil, err
	}
	defer rotatedAuditFile.Close()
	noRotateLogs := false
	if err := o.Executor.Execute("POST", rotatedRequest.URL(), o.Config, o.In, rotatedAuditFile, o.ErrOut, t.Raw, sizeQueue); err != nil {
		if strings.Contains(err.Error(), "command terminated with exit code 2") {
			noRotateLogs = true
		} else {
			return nil, fmt.Errorf("failed to get rotated audit logs for %s: %v", apiserverName, err)
		}
	}

	if !noRotateLogs {
		files = append(files, rotatedAuditFile.Name())
	}

	// second copy the live audit file which might come corrupted
	liveRequest := restClient.Post().
		Resource("pods").
		Name(apiserverName).
		Namespace("openshift-kube-apiserver").
		SubResource("exec")
	liveRequest.VersionedParams(&corev1.PodExecOptions{
		Container: "kube-apiserver",
		TTY:       t.Raw,
		Stdout:    true,
		Command:   []string{"/bin/bash", "-c", "cd /tmp && cp --remove-destination /var/log/kube-apiserver/audit.log audit.log && tar -czO audit.log && rm -f audit.log"},
	}, scheme.ParameterCodec)

	liveAuditFile, err := ioutil.TempFile(apiServerTargetDirectory, "audit-log")
	if err != nil {
		return nil, err
	}
	defer liveAuditFile.Close()
	if err := o.Executor.Execute("POST", liveRequest.URL(), o.Config, o.In, liveAuditFile, o.ErrOut, t.Raw, sizeQueue); err != nil {
		return nil, err
	}
	files = append(files, liveAuditFile.Name())

	return files, nil
}

func (o *Options) Run(ctx context.Context) error {
	pods, err := o.findAPIServerPods(ctx)
	if err != nil {
		return err
	}
	klog.V(4).Infof("Got Kubernetes API server pods: %s", strings.Join(pods, ","))

	for _, p := range pods {
		klog.V(4).Infof("Getting audit logs for %s ...", p)
		_, err := o.getAPIServerLogs(p)
		if err != nil {
			return err
		}
	}

	klog.Infof("Audit logs successfully downloaded to %s", o.targetDirectory)
	return nil
}

func (o *Options) Validate() error {
	if len(o.targetDirectory) == 0 {
		return fmt.Errorf("output directory must be set")
	}
	return nil
}
