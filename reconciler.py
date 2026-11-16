from kubernetes import client, config, watch
from rich.console import Console

console = Console()
previous_drift = {}

def create_pod(core_api, name, namespace, image):
    # Generate a unique name for each pod
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            generate_name=f"{name}-",
            labels={
                "app.kubernetes.io/managed-by": name
            }
        ),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="app",
                    image=image
                )
            ],
            restart_policy="Always"
        )
    )

    # send the pod creation request to kubernetes
    created_pod = core_api.create_namespaced_pod(
        namespace=namespace,
        body=pod
    )

    console.print(
        f"[green]Created pod: {created_pod.metadata.name}[/green]"
    )

def reconcile(core_api, resource):
    # Extract information from the custom resource
    name = resource["metadata"]["name"]
    namespace = resource["metadata"]["namespace"]
    desired_replicas = resource["spec"]["replicas"]
    image = resource["spec"]["image"]

    # Find pods managed by this resource
    label_selector = f"app.kubernetes.io/managed-by={name}"

    pods = core_api.list_namespaced_pod(
        namespace=namespace,
        label_selector=label_selector
    ).items

    # Count active pods (Pending or Running, excluding terminating pods)
    active_pods = [
        pod for pod in pods
        if pod.status.phase in ("Pending", "Running")
        and pod.metadata.deletion_timestamp is None
    ]

    observed_replicas = len(active_pods)

    # Calculate drift
    drift = desired_replicas - observed_replicas

    # Track previous drift for each resource
    resource_key = f"{namespace}/{name}"
    last_drift = previous_drift.get(resource_key, 0)

    # Only log when drift is detected
    if drift != 0:
        console.print(
            f"[RECONCILE LOOP] Resource: {name} | "
            f"Desired: {desired_replicas} | "
            f"Observed: {observed_replicas} | "
            f"Drift: {drift}"
        )

    # Scale up if there are too few pods
    if drift > 0:
        console.print(
            f"Scale-Up Event: Provisioning {drift} pod(s) to resolve drift..."
        )

        for _ in range(drift):
            create_pod(core_api, name, namespace, image)

    # Scale down if there are too many pods
    elif drift < 0:
        excess = abs(drift)

        console.print(
            f"Scale-Down Event: Removing {excess} excess pod(s)..."
        )

        for pod in active_pods[:excess]:
            core_api.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=namespace
            )

            console.print(
                f"[yellow]Deleted pod: {pod.metadata.name}[/yellow]"
            )

    # Report equilibrium only when recovering from drift
    elif last_drift != 0:
        console.print(
            f"[green]EQUILIBRIUM RESTORED: {name} "
            f"({observed_replicas}/{desired_replicas} pods)[/green]"
        )

    # Save the current drift for the next reconciliation
    previous_drift[resource_key] = drift

    return drift


def main():
    # Load the kubernetes configuration from our local machine
    config.load_kube_config()

    # initialize kubernetes API clients
    core_api = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    console.print(
        "[bold green]=== COSC 3657 Custom Kubernetes Reconciler Started ===[/bold green]"
    )

    while True:
        try:
            # Retrieve all ResilientDeployment resources
            resources = custom_api.list_namespaced_custom_object(
                group="cosc3657.tu.ca",
                version="v1alpha1",
                namespace="default",
                plural="resilientdeployments"
            )

            # Reconcile every resource
            for resource in resources.get("items", []):
                reconcile(core_api, resource)

            # Watch for changes to our custom resources
            resource_watch = watch.Watch()

            for event in resource_watch.stream(
                custom_api.list_namespaced_custom_object,
                group="cosc3657.tu.ca",
                version="v1alpha1",
                namespace="default",
                plural="resilientdeployments",
                timeout_seconds=2
            ):
                if event["type"] in ("ADDED", "MODIFIED"):
                    reconcile(core_api, event["object"])

        except KeyboardInterrupt:
            console.print(
                "\n[yellow]Reconciler stopped by user.[/yellow]"
            )
            break

        except Exception as error:
            console.print(f"[red]Reconciliation error: {error}[/red]")
            import time
            time.sleep(2)


if __name__ == "__main__":
    main()