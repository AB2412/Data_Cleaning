# Creator: Alex Kuzmenko

def clear_name_part(str)
  return '' unless str.index(/[a-zA-Z0-9]/)
  str = str.gsub(/(^[^a-zA-Z0-9]+)|([^a-zA-Z0-9]+$)/, '')
  str = str.gsub(/&/, ' & ').gsub('  ', ' ') if str.index(/&/)
  str.multi_gsub!(/(^(,|\.))|(,$)/, '')
  str = clamped_comma(str)
  parts = str.split ' '

  parts.map! do |el|
    el = F::Cleaner.person_clean(el, reverse = false)

    manual_names_corrections.each_pair do |k, v|
      if el.match(k)
        el = v

        break
      end
    end

    el
  end

  parts.join(' ')
end

def make_small_m_name(str)
  return '' unless str
  return '' if str.strip.empty?

  str = str.slice(/([A-Z]\.)+/i) || (str.index(/[A-Z]/) ? str[str.index(/[A-Z]/)] : '')
  "#{str}.".multi_gsub!('..', '.')
end

def manual_names_corrections
  {/\bLTD\b\.?/i => 'Ltd.',
   /\bINCO?R?P?O?R?A?T?E?D?\b\.?/i => 'Inc.',
   /\bCORP(ORATION)?\b\.?/i => 'Corp.',
   /\bCOM?P?\b\.?/i => 'Co.',
   /\bLLC\b\.?/i => 'LLC',
   /\bLLP\b\.?/i => 'LLP',
   /\bPLLC\b\.?/i => 'PLLC'}
end

def disclosure_names_query
  <<~SQL
    SELECT
      id,
      first_name_40 AS fn,
      mid_int_42 AS mn,
      last_name_44 AS ln
    FROM ny_campaign_finance_disclosure
    WHERE full_name_clean IS NULL;
  SQL
end

def disclosure_names_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    names_to_clear = db01.query(disclosure_names_query).to_a

    semaphore = Mutex.new
    threads = Array.new(10) do
      Thread.new do
        db01_ins = C::Mysql.on(DB01, 'usa_raw')

        loop do
          hash = nil
          semaphore.synchronize { hash = names_to_clear.pop }

          break unless hash

          begin
            id     = hash['id']
            new_ln = hash['ln'] ? clear_name_part(hash['ln']) : ''
            new_fn = hash['fn'] ? clear_name_part(hash['fn']) : ''
            new_mn = F::Cleaner.person_clean(make_small_m_name(hash['mn']))
            new_full_name = "#{new_fn} #{new_ln}".strip

            next if new_full_name.empty? || new_full_name.index(/^[^a-zA-Z]+/)

            new_full_name = "#{new_fn} #{new_mn} #{new_ln}".multi_gsub!('  ').strip

            update_query = <<~SQL
              UPDATE ny_campaign_finance_disclosure
              SET full_name_clean = #{new_full_name.dump}
              WHERE id = #{id};
            SQL
            db01_ins.query(update_query)
          rescue Mysql2::Error => e
            p e
          rescue Exception => e
            puts "!!\nException of SQL update algorithm\n!!"
            p e
          end
        end # loop
        db01_ins.close if db01_ins
      end # thread
    end # threads
    threads.each(&:join)
  rescue Exception
    p "Something went wrong"
  end
  db01.close if db01
  puts 'Done'
end
