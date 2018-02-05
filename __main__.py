from cloudshell.workflow.orchestration.sandbox import Sandbox
from cloudshell.workflow.orchestration.setup.default_setup_orchestrator import DefaultSetupWorkflow
from cloudshell.workflow.orchestration.setup.default_setup_logic import DefaultSetupLogic
from cloudshell.workflow.orchestration.app import App

from multiprocessing.pool import ThreadPool
from threading import Lock

def main():
    sandbox = Sandbox()
    sandbox.logger.info('starting main')
    sandbox.automation_api.WriteMessageToReservationOutput(reservationId=sandbox.id,
                                                           message='Starting apps based on their priority')

    DefaultSetupWorkflow().register(sandbox, enable_connectivity=False,enable_configuration=False)  # Disable OOTB configuration
    sandbox.workflow.add_to_connectivity(function=start_apps,
                                          components=sandbox.components.apps)
    sandbox.execute_setup()

def configure_app(pri_type,api, reservation_id, logger,apps,parallel, appConfigurations=[]):

    if len(apps) == 0 :
        api.WriteMessageToReservationOutput(reservation_id, 'No {} app/s to configure'.format(pri_type))
        return

    logger.info('App configuration started ...')

    try:
        if not parallel:
            for app in apps:

                configuration_result = api.ConfigureApps(reservationId=reservation_id, printOutput=True,
                                                 appConfigurations=App(app).app_request.appConfiguration)
                api.WriteMessageToReservationOutput(reservation_id,App(app).app_request.appConfiguration )
        else:
            app_configs=[]
            for app in apps:
                app_configs.append(App(app).app_request.appConfiguration)
            configuration_result = api.ConfigureApps(reservationId=reservation_id, printOutput=True)

        if not configuration_result.ResultItems:
            api.WriteMessageToReservationOutput(reservation_id, 'No {} app/s to configure'.format(pri_type))
            return

        failed_apps = []
        for conf_res in configuration_result.ResultItems:
            if conf_res.Success:
                message = "App '{0}' configured successfully".format(conf_res.AppName)
                logger.info(message)
            else:
                message = "App '{0}' configuration failed due to {1}".format(conf_res.AppName,
                                                                             conf_res.Error)
                logger.error(message)
                failed_apps.append(conf_res.AppName)

        if not failed_apps:
            api.WriteMessageToReservationOutput(reservationId=reservation_id, message=
            'Apps were configured successfully.')
        else:
            api.WriteMessageToReservationOutput(reservationId=reservation_id, message=
            'Apps: {0} configuration failed. See logs for more details'.format(
                ",".join(failed_apps)))
            raise Exception("Configuration of apps failed see logs.")
    except Exception as ex:
        logger.error("Error configuring apps. Error: {0}".format(str(ex)))
        raise

    return

def power_on_resources(resources , deploy_results, api, sandbox, parallel):

    temp_power_on = []

    if len(resources) == 0:
        api.WriteMessageToReservationOutput(
            reservationId=sandbox.id,
            message='No resources to power on')
        return

    lock = Lock()
    message_status = {
        "power_on": False,
        "wait_for_ip": False
    }

    if not parallel:
        for resource in resources:
            sandbox.automation_api.WriteMessageToReservationOutput(reservationId=sandbox.id,
                                                               message='* Powering ON : '+str(resource.Name))

            temp_power_on.append(DefaultSetupLogic._power_on_refresh_ip(api, lock, message_status, resource,
                                                                   deploy_results, {}, sandbox.id, sandbox.logger))
    else:
        pool = ThreadPool(len(resources))
        sandbox.automation_api.WriteMessageToReservationOutput(sandbox.id,'* Powering ON All others:...'+str(len(resources)))

        temp_power_on = [pool.apply_async(DefaultSetupLogic._power_on_refresh_ip,
                                          (api, lock, message_status, resource, deploy_results, {},
                                           sandbox.id, sandbox.logger))
                         for resource in resources]

        pool.close()
        pool.join()

    return temp_power_on

def start_apps(sandbox, components):
    """
    :param Sandbox sandbox:
    :return:
    """
    sandbox.automation_api.WriteMessageToReservationOutput(reservationId=sandbox.id,
                                                           message='start_apps started')

    # L2 Connectivity

    api = sandbox.automation_api
    reservation_details = api.GetReservationDetails(sandbox.id)
    DefaultSetupLogic.connect_all_routes_in_reservation(api=api,
                                                        reservation_details=reservation_details,
                                                        reservation_id=sandbox.id,
                                                        resource_details_cache={},
                                                        logger=sandbox.logger)
    # Starting Highest priority apps

    power_on_results = []
    high_apps = []
    medium_apps = []
    low_apps = []
    nopri_apps = []

    deploy_results = DefaultSetupLogic.deploy_apps_in_reservation(api=api,
                                                                  reservation_details=reservation_details,
                                                                  reservation_id=sandbox.id,
                                                                  logger=sandbox.logger)

    api.WriteMessageToReservationOutput(sandbox.id,'Number of Apps = '+str(len(sandbox.components.apps)))

    resources = reservation_details.ReservationDescription.Resources

    for res in resources:
        if "PRI_0" in res.Name:
            high_apps.append(res)
        if "PRI_1" in res.Name:
            medium_apps.append(res)
        if "PRI_2" in res.Name:
            low_apps.append(res)
        if "PRI_" not in res.Name:
            nopri_apps.append(res)

    if len(high_apps) > 0:
        api.WriteMessageToReservationOutput(sandbox.id,'Powering On High Priority Apps ')
        power_on_results.append(power_on_resources(high_apps, deploy_results, api, sandbox, False))
    if len(medium_apps) > 0:
        api.WriteMessageToReservationOutput(sandbox.id,'Powering On Medium Priority Apps')
        power_on_results.append(power_on_resources(medium_apps, deploy_results, api, sandbox,False))
    if len(low_apps) > 0:
        api.WriteMessageToReservationOutput(sandbox.id,'Powering On Low Priority Apps')
        power_on_results.append(power_on_resources(low_apps, deploy_results, api, sandbox,False))
    if len(nopri_apps) > 0:
        api.WriteMessageToReservationOutput(sandbox.id,'Powering On No(Lowest) Priority Apps')
        power_on_results.append(power_on_resources(nopri_apps, deploy_results, api, sandbox,True))

    DefaultSetupLogic.validate_all_apps_deployed(deploy_results,sandbox.logger)

    configure_app("High Priority",api, sandbox.id, sandbox.logger, high_apps, False)
    configure_app("Medium Priority",api, sandbox.id, sandbox.logger, medium_apps, False)
    configure_app("Low Priority",api, sandbox.id, sandbox.logger, low_apps, False)
    configure_app("None Priority",api, sandbox.id, sandbox.logger, nopri_apps, True)

main()
