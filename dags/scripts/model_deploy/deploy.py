


# define callback functions
def get_endpoint_config(**context):
    model_version = context['ti'].xcom_pull(task_ids='get_model_details', key='model_version')
    endpoint_config_name = f'delivery-delay-config-{model_version}'
    return {
        'EndpointConfigName': endpoint_config_name,
        'ProductionVariants': [{
            'InstanceType': 'ml.t2.medium',  # Adjust based on needs
            'InitialInstanceCount': 1,
            'ModelName': f'DeliveryDelayModelSeoul',
            'VariantName': 'AllTraffic',
        }],
    }

def get_endpoint(**context):
    model_version = context['ti'].xcom_pull(task_ids='get_model_details', key='model_version')
    endpoint_config_name = f'delivery-delay-config-{model_version}'
    return {
        'EndpointName': 'delivery-delay-endpoint',  # Fixed name for production (updates existing endpoint)
        'EndpointConfigName': endpoint_config_name,
    }

