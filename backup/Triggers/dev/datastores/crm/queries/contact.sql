<fetch version="1.0" output-format="xml-platform" mapping="logical" distinct="false">
	<entity name="contact">
		<attribute name="contactid" />
		<attribute name="emailaddress1" />
		<attribute name="emailaddress3" />
		<attribute name="lastname" />
		<attribute name="firstname" />
		<attribute name="yc_omniaccountid" />
		<attribute name="address1_postofficebox" />
		<attribute name="birthdate" />
		<attribute name="fullname" />
		<attribute name="dot_primaryaddresscity" />
		<attribute name="address1_composite" />
		<attribute name="dot_primaryaddresscountry" />
		<attribute name="dot_contacttravelboxid" />
		<attribute name="mobilephone" />
		<attribute name="gendercode" />
		<attribute name="yc_yasid" />
		<attribute name="fax" />
		<attribute name="emailaddress2" />
		<attribute name="telephone1" />
		<attribute name="parentcustomerid" />
		<attribute name="dot_nationalityid" />
		<attribute name="customertypecode" />
		<attribute name="dot_preferredlanguage" />
		<attribute name="statuscode" />
		<attribute name="jobtitle" />
		<attribute name="yc_customermdmid" />
		<attribute name="salutation" />
		<attribute name="createdon" />
		<attribute name="modifiedon" />
		<filter type="and">
			<condition attribute="modifiedon" operator="on-or-after" value="<<modifiedon>>"/>
		</filter>
	</entity>
</fetch>
