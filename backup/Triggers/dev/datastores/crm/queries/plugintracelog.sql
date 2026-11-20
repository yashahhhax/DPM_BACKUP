<fetch version="1.0" output-format="xml-platform" mapping="logical" distinct="false">
    <entity name="plugintracelog">
        <attribute name="plugintracelogid" />
        <attribute name="messagename" />
        <attribute name="messageblock" />
        <attribute name="issystemcreated" />
        <attribute name="operationtype" />
        <attribute name="typename" />
        <attribute name="performanceexecutionstarttime" />
        <attribute name="correlationid" />
        <order attribute="messagename" descending="true" />
        <filter type="and">
            <filter type="or">
                <condition attribute="typename" operator="like" value="%YC.CRM.Plugin.OMNIIntegration.SearchOmniAccountByEmail%" />
                <condition attribute="typename" operator="like" value="%YC.CRM.Plugin.SCVIntegration.CreateEmployeeRelationshipPlugin%" />
                <condition attribute="typename" operator="like" value="%YC.CRM.EmployeeFacePassValidation.ValidateOmni%" />
            </filter>
        </filter>
    </entity>
</fetch>
