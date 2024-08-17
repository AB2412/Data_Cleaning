# Creator:      Sergii Butrymenko
# Dataset Name: PARCC Schools
# Task #:       69
# Migrated:     April 2022

# ruby mlc.rb --tool="clean::il::parcc_school"

def execute(options = {})
  route_db01 = C::Mysql.on(DB01, 'il_raw')
  query = "SELECT id, school_name, clean_school_name FROM PARCC_school WHERE clean_school_name IS NULL;"
  root_data = route_db01.query(query).to_a
  counter = 0
  root_data.map do |record|
    counter += 1
    school_name = record['school_name']
    school_name = school_name.gsub(/\bSCH\b/, 'School')
    school_name = school_name.gsub(/\bELEM\b/, 'Elementary')
    school_name = school_name.split.map(&:capitalize).join(' ')
    school_name = school_name.gsub(/([A-Z]) ([A-Z]) /) { Regexp.last_match(1).to_s.strip + '.' + Regexp.last_match(2).to_s.strip + '. '}
    school_name = school_name.gsub(/([A-Z]) /) { Regexp.last_match(1).to_s.strip + '. '}
    school_name = school_name.gsub(/\b -\b/, ' - ')
    school_name = school_name.gsub(/\b- \b/, ' - ')
    school_name = school_name.gsub(/\bHs\b/, 'High School')
    school_name = school_name.gsub(/\bH S\b/, 'High School')
    school_name = school_name.gsub(/\bSr\b/, 'Senior')
    school_name = school_name.gsub(/\bJr\b/, 'Junior')
    school_name = school_name.gsub(/\bAcad\b/, 'Academy')
    school_name = school_name.gsub(/\bEs\b/, 'Elementary School')
    school_name = school_name.gsub(/\bE S\b/, 'Elementary School')
    school_name = school_name.gsub(/\bCtr\b/, 'Center')
    school_name = school_name.gsub(/\bSchl\b/, 'School')
    school_name = school_name.gsub('&', 'and')
    school_name = school_name.gsub(/\bJhs\b/, 'Junior High School')
    school_name = school_name.gsub(/\bCics\b/, 'CICS')
    school_name = school_name.gsub(/\bChtr\b/, 'Charter')
    school_name = school_name.gsub(/\bYccs\b/, 'YCCS')
    school_name = school_name.gsub(/-([a-z])/) { '-' + Regexp.last_match(1).to_s.upcase }
    school_name = school_name.gsub(/\/([a-z])/) { '/' + Regexp.last_match(1).to_s.upcase }

    update_query = "UPDATE PARCC_school SET clean_school_name = \"#{school_name}\" where id = #{record['id']};"
    puts update_query
    puts
    route_db01.query(update_query)
    # puts("#{record['school_name']} >> #{school_name} >> #{MiniLokiC::Formatize::Cleaner.org_clean(record['school_name'])}")
    # puts "--------------------------------------------------------------------"
  end
  if counter.zero?
    message_to_slack('No schools to clean', :info)
  else
    message_to_slack("#{counter} new school(s) were added into *db01.il_raw.clean_school_name*. Check them please!", :warning)
  end
  route_db01.close
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #10] Florida Professional Licenses* \n>#{type} #{message}",
    as_user: true
  )
end
